#pragma once
#include <semaphore.h>
#include <cstddef>

constexpr const char* SHM_NAME = "/ring_buffer_shm";
constexpr size_t RING_CAPACITY = 1024; // Кількість слотів у буфері
constexpr size_t MESSAGE_SIZE = 256;   // Розмір одного повідомлення

struct SharedContext {
    sem_t sem_free;
    sem_t sem_used;
    // Створюємо масив рядків: 1024 слоти по 256 байт кожен
    char messages[RING_CAPACITY][MESSAGE_SIZE];
};