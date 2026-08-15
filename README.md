#Thread Pool - High-Performance Task Queue

Библиотека пула потоков для эффективного выполнения параллельных задач на C.

Описание

Thread Pool реализует шаблон проектирования "Пул потоков" для оптимизации выполнения множества коротких задач без накладных расходов на создание потоков.
Технологии

    Язык: C (C99)
    API: POSIX Threads (pthreads)
    Примитивы: mutex, condition variables, atomic operations

Возможности

    Ленивое создание потоков (по мере необходимости)
    Потокобезопасная очередь задач
    Ограничение максимального числа потоков
    Повторное использование задач (re-push)
    thread_task_join() - ожидание завершения
    thread_task_timed_join() - ожидание с таймаутом
    thread_task_detach() - автоматическое удаление

Быстрый старт
Сборка:
gcc -Wall -Wextra -Werror -pthread -o test test.c thread_pool.c
Запуск тестов:
./test
API
Создание пула:
struct thread_pool *pool;
thread_pool_new(max_threads, &pool);
Создание и выполнение задачи:
struct thread_task *task;
thread_task_new(&task, my_function, arg);
thread_pool_push_task(pool, task);
thread_task_join(task, &result);
thread_task_delete(task);
Архитектура

    Worker Threads: Потоки, ожидающие задачи из очереди
    Task Queue: Потокобезопасная очередь с мьютексом
    Condition Variables: Для сигнализации о новых задачах
    Atomic Counters: Для отслеживания состояния

Особенности

    Отсутствие busy-wait (используются condition variables)
    Корректная обработка гонок (race conditions)
    Отсутствие утечек памяти
    Поддержка до 100,000 задач

Тесты
Включают:

    Стресс-тесты (1000+ задач)
    Тесты на гонки данных
    Тесты таймаутов
    Тесты detach

