#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
#include <chrono>
#include <vector>
#include "common.hpp"

int main(int argc, char* argv[]) {
    // 1. Читаємо розмір payload з аргументів CLI
    if (argc != 2) {
        std::cerr << "Використання: ./producer <розмір_payload_у_байтах>\n";
        return 1;
    }
    uint32_t payload_size = std::stoul(argv[1]);

    // 2. Вираховуємо загальний розмір спільної пам'яті
    size_t slot_size = sizeof(PacketHeader) + payload_size;
    size_t total_shm_size = sizeof(SharedContext) + (RING_CAPACITY * slot_size);

    int fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
    if (fd == -1) { perror("shm_open"); return 1; }
    
    ftruncate(fd, total_shm_size); // Виділяємо точну кількість пам'яті
    
    void* ptr = mmap(0, total_shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); return 1; }
    
    // 3. Розмічаємо пам'ять
    SharedContext* ctx = static_cast<SharedContext*>(ptr);
    uint8_t* buffer_start = static_cast<uint8_t*>(ptr) + sizeof(SharedContext);

    // Ініціалізація
    sem_init(&ctx->sem_free, 1, RING_CAPACITY); 
    sem_init(&ctx->sem_used, 1, 0);
    ctx->configured_payload_size = payload_size; // Зберігаємо розмір для Consumer'а

    std::vector<uint8_t> dummy_data(payload_size, 0xAB); // Тестові дані (заповнені байтом 0xAB)
    size_t head = 0;

    std::cout << "[Producer] Старт. Розмір payload: " << payload_size << " байт. Генеруємо 5000 пакетів...\n";

    for (uint64_t i = 1; i <= 5000; ++i) {
        sem_wait(&ctx->sem_free); 

        // 4. Обчислюємо, де саме в пам'яті знаходиться поточний слот
        uint8_t* current_slot = buffer_start + (head * slot_size);
        PacketHeader* header = reinterpret_cast<PacketHeader*>(current_slot);
        uint8_t* payload_ptr = current_slot + sizeof(PacketHeader);

        // 5. Заповнюємо заголовок (Метадані)
        header->seq_num = i;
        header->timestamp_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::high_resolution_clock::now().time_since_epoch()).count();
        header->payload_size = payload_size;
        
        // Копіюємо дані та рахуємо контрольну суму
        std::memcpy(payload_ptr, dummy_data.data(), payload_size);
        header->checksum = calculate_checksum(payload_ptr, payload_size);

        sem_post(&ctx->sem_used); 
        head = (head + 1) % RING_CAPACITY;
    }

    std::cout << "[Producer] Успішно завершено.\n";
    return 0;
}