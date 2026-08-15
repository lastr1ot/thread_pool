#define _GNU_SOURCE  // Должно быть самой первой строкой в файле!

#include "thread_pool.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>

struct thread_task {
	thread_task_f function;
	void *arg;
	void *result;
	
	bool has_been_pushed;
	bool is_in_pool;
	bool is_running;
	bool is_finished;
	bool is_detached;
	
	pthread_mutex_t mutex;
	pthread_cond_t cond;
};

struct thread_pool {
	int max_threads;
	int active_threads;
	int pending_tasks;

	struct thread_task **queue;
	int queue_capacity;
	int queue_head;
	int queue_tail;
	int queue_count;

	pthread_mutex_t mutex;
	pthread_cond_t cond;
	bool is_destroying;

	pthread_t *threads;
};

static void *worker_thread(void *arg) {
	struct thread_pool *pool = (struct thread_pool *)arg;

	while (1) {
		pthread_mutex_lock(&pool->mutex);
		
		while (pool->queue_count == 0 && !pool->is_destroying) {
			pthread_cond_wait(&pool->cond, &pool->mutex);
		}

		if (pool->is_destroying && pool->queue_count == 0) {
			pool->active_threads--;
			pthread_mutex_unlock(&pool->mutex);
			return NULL;
		}

		struct thread_task *task = pool->queue[pool->queue_head];
		pool->queue_head = (pool->queue_head + 1) % pool->queue_capacity;
		pool->queue_count--;
		pthread_mutex_unlock(&pool->mutex);

		pthread_mutex_lock(&task->mutex);
		task->is_running = true;
		pthread_mutex_unlock(&task->mutex);

		void *res = task->function(task->arg);

		pthread_mutex_lock(&pool->mutex);
		pool->pending_tasks--;
		pthread_mutex_unlock(&pool->mutex);

		pthread_mutex_lock(&task->mutex);
		task->result = res;
		task->is_running = false;
		task->is_finished = true;
		task->is_in_pool = false;
		bool detach = task->is_detached;
		pthread_cond_broadcast(&task->cond);
		pthread_mutex_unlock(&task->mutex);

		if (detach) {
			pthread_mutex_destroy(&task->mutex);
			pthread_cond_destroy(&task->cond);
			free(task);
		}
	}
	return NULL;
}

int thread_pool_new(int max_thread_count, struct thread_pool **pool) {
	if (max_thread_count <= 0 || max_thread_count > TPOOL_MAX_THREADS) {
		return TPOOL_ERR_INVALID_ARGUMENT;
	}

	struct thread_pool *p = calloc(1, sizeof(struct thread_pool));
	if (!p) return TPOOL_ERR_INVALID_ARGUMENT;

	p->max_threads = max_thread_count;
	p->active_threads = 0;
	p->pending_tasks = 0;
	p->queue_capacity = TPOOL_MAX_TASKS;
	p->queue = calloc(p->queue_capacity, sizeof(struct thread_task *));
	if (!p->queue) {
		free(p);
		return TPOOL_ERR_INVALID_ARGUMENT;
	}
	p->queue_head = 0;
	p->queue_tail = 0;
	p->queue_count = 0;
	p->is_destroying = false;
	p->threads = calloc(p->max_threads, sizeof(pthread_t));
	if (!p->threads) {
		free(p->queue);
		free(p);
		return TPOOL_ERR_INVALID_ARGUMENT;
	}

	pthread_mutex_init(&p->mutex, NULL);
	
	pthread_condattr_t attr;
	pthread_condattr_init(&attr);
	pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
	pthread_cond_init(&p->cond, &attr);
	pthread_condattr_destroy(&attr);

	*pool = p;
	return 0;
}

int thread_pool_thread_count(const struct thread_pool *pool) {
	return pool->active_threads;
}

int thread_pool_delete(struct thread_pool *pool) {
	pthread_mutex_lock(&pool->mutex);
	if (pool->pending_tasks > 0) {
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_HAS_TASKS;
	}
	
	pool->is_destroying = true;
	pthread_cond_broadcast(&pool->cond);
	
	int threads_to_join = pool->active_threads;
	pthread_mutex_unlock(&pool->mutex);

	for (int i = 0; i < threads_to_join; i++) {
		pthread_join(pool->threads[i], NULL);
	}

	pthread_mutex_destroy(&pool->mutex);
	pthread_cond_destroy(&pool->cond);
	free(pool->threads);
	free(pool->queue);
	free(pool);
	return 0;
}

int thread_pool_push_task(struct thread_pool *pool, struct thread_task *task) {
	pthread_mutex_lock(&pool->mutex);
	
	if (pool->queue_count >= TPOOL_MAX_TASKS) {
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_TOO_MANY_TASKS;
	}

	pool->queue[pool->queue_tail] = task;
	pool->queue_tail = (pool->queue_tail + 1) % pool->queue_capacity;
	pool->queue_count++;
	pool->pending_tasks++;

	task->has_been_pushed = true;
	task->is_in_pool = true;
	task->is_finished = false;
	task->is_running = false;
	task->is_detached = false;

	// ИСПРАВЛЕНИЕ: Создаем новый поток только если задач больше, чем потоков,
	// и мы еще не достигли лимита max_threads.
	if (pool->active_threads < pool->pending_tasks && pool->active_threads < pool->max_threads) {
		int thread_idx = pool->active_threads;
		pool->active_threads++;
		pthread_mutex_unlock(&pool->mutex);

		if (pthread_create(&pool->threads[thread_idx], NULL, worker_thread, pool) != 0) {
			pthread_mutex_lock(&pool->mutex);
			pool->active_threads--;
			pool->pending_tasks--;
			pool->queue_count--;
			pool->queue_tail = (pool->queue_tail - 1 + pool->queue_capacity) % pool->queue_capacity;
			task->has_been_pushed = false;
			task->is_in_pool = false;
			pthread_mutex_unlock(&pool->mutex);
			return TPOOL_ERR_INVALID_ARGUMENT;
		}
	} else {
		// Иначе просто будим один из спящих потоков
		pthread_cond_signal(&pool->cond);
		pthread_mutex_unlock(&pool->mutex);
	}
	
	return 0;
}

int thread_task_new(struct thread_task **task, thread_task_f function, void *arg) {
	struct thread_task *t = calloc(1, sizeof(struct thread_task));
	if (!t) return TPOOL_ERR_INVALID_ARGUMENT;

	t->function = function;
	t->arg = arg;
	t->has_been_pushed = false;
	t->is_in_pool = false;
	t->is_running = false;
	t->is_finished = false;
	t->is_detached = false;

	pthread_mutex_init(&t->mutex, NULL);
	
	pthread_condattr_t attr;
	pthread_condattr_init(&attr);
	pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
	pthread_cond_init(&t->cond, &attr);
	pthread_condattr_destroy(&attr);

	*task = t;
	return 0;
}

bool thread_task_is_finished(const struct thread_task *task) {
	return task->is_finished;
}

bool thread_task_is_running(const struct thread_task *task) {
	return task->is_running;
}

int thread_task_join(struct thread_task *task, void **result) {
	pthread_mutex_lock(&task->mutex);
	if (!task->has_been_pushed) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
	
	while (!task->is_finished) {
		pthread_cond_wait(&task->cond, &task->mutex);
	}
	
	if (result) {
		*result = task->result;
	}
	pthread_mutex_unlock(&task->mutex);
	return 0;
}

#if NEED_TIMED_JOIN
int thread_task_timed_join(struct thread_task *task, double timeout, void **result) {
	pthread_mutex_lock(&task->mutex);
	if (!task->has_been_pushed) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}

	if (timeout <= 0.0) {
		if (task->is_finished) {
			if (result) *result = task->result;
			pthread_mutex_unlock(&task->mutex);
			return 0;
		} else {
			pthread_mutex_unlock(&task->mutex);
			return TPOOL_ERR_TIMEOUT;
		}
	}

	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	
	long sec = (long)timeout;
	long nsec = (long)((timeout - sec) * 1000000000.0);
	ts.tv_sec += sec;
	ts.tv_nsec += nsec;
	while (ts.tv_nsec >= 1000000000) {
		ts.tv_sec++;
		ts.tv_nsec -= 1000000000;
	}

	int rc = 0;
	while (!task->is_finished) {
		rc = pthread_cond_timedwait(&task->cond, &task->mutex, &ts);
		if (rc == ETIMEDOUT) {
			pthread_mutex_unlock(&task->mutex);
			return TPOOL_ERR_TIMEOUT;
		}
	}

	if (result) {
		*result = task->result;
	}
	pthread_mutex_unlock(&task->mutex);
	return 0;
}
#endif

int thread_task_delete(struct thread_task *task) {
	pthread_mutex_lock(&task->mutex);
	if (task->is_in_pool) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_IN_POOL;
	}
	pthread_mutex_unlock(&task->mutex);
	
	pthread_mutex_destroy(&task->mutex);
	pthread_cond_destroy(&task->cond);
	free(task);
	return 0;
}

#if NEED_DETACH
int thread_task_detach(struct thread_task *task) {
	pthread_mutex_lock(&task->mutex);
	if (!task->has_been_pushed) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
	
	if (task->is_finished) {
		pthread_mutex_t m = task->mutex;
		pthread_cond_t c = task->cond;
		pthread_mutex_unlock(&task->mutex);
		
		pthread_mutex_destroy(&m);
		pthread_cond_destroy(&c);
		free(task);
	} else {
		task->is_detached = true;
		pthread_mutex_unlock(&task->mutex);
	}
	return 0;
}
#endif