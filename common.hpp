#pragma once
#include <semaphore.h>
#include <cstddef>
#include <cstdint>
#include <atomic>

constexpr const char* SHM_NAME = "/ring_buffer_shm";
constexpr const char* SEM_FREE_NAME = "/ring_sem_free"; // Нове
constexpr const char* SEM_USED_NAME = "/ring_sem_used"; // Нове
constexpr size_t RING_CAPACITY = 1024;

#pragma pack(push, 1)
struct PacketHeader {
    uint64_t seq_num; 
    uint64_t timestamp_ns; 
    uint64_t payload_size; 
    uint64_t checksum; 
};
#pragma pack(pop)

struct SharedContext {
    uint32_t configured_payload_size; 
    std::atomic<bool> producer_active;
};

inline uint32_t calculate_checksum(const uint8_t* data, size_t size) {
    uint32_t checksum = 0;
    for (size_t i = 0; i < size; ++i) checksum += data[i];
    return checksum;
}