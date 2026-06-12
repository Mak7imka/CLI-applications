#pragma once
#include <semaphore.h>
#include <cstddef>
#include <cstdint>

constexpr const char* SHM_NAME = "/ring_buffer_shm";
constexpr size_t RING_CAPACITY = 1024; // Кількість слотів у буфері
constexpr size_t MESSAGE_SIZE = 256;   // Розмір одного повідомлення

#pragma pack(push, 1) // Вимикаємо вирівнювання для економії пам'яті
struct PacketHeader {
    uint64_t seq_num; // Номер пакета
    uint64_t timestamp_ns; // Час створення пакета
    uint64_t payload_size; // Розмір корисних даних
    uint64_t checksum; // Контрольна сума для перевірки цілісності
};
#pragma pack(pop)


struct SharedContext {
    sem_t sem_free;
    sem_t sem_used;
    uint32_t configured_payload_size; // Щоб Consumer дізнався розмір пакета
};

inline uint32_t calculate_checksum(const uint8_t* data, size_t size) {
    uint32_t checksum = 0;
    for (size_t i = 0; i < size; ++i) {
        checksum += data[i];
    }
    return checksum;
}
