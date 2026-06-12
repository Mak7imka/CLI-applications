#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include "common.hpp"

int main() {
    int fd = shm_open(SHM_NAME, O_RDWR, 0666);
    if (fd == -1) { 
        std::cerr << "Помилка: Запусти Producer спочатку!\n"; 
        return 1; 
    }

    // КРОК А: Мапимо ТІЛЬКИ SharedContext, щоб дізнатися розмір payload
    void* temp_ptr = mmap(0, sizeof(SharedContext), PROT_READ, MAP_SHARED, fd, 0);
    SharedContext* temp_ctx = static_cast<SharedContext*>(temp_ptr);
    uint32_t payload_size = temp_ctx->configured_payload_size;
    munmap(temp_ptr, sizeof(SharedContext)); // Відключаємо тимчасовий вказівник

    // КРОК Б: Тепер ми знаємо розмір! Мапимо пам'ять повністю
    size_t slot_size = sizeof(PacketHeader) + payload_size;
    size_t total_shm_size = sizeof(SharedContext) + (RING_CAPACITY * slot_size);

    void* ptr = mmap(0, total_shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    SharedContext* ctx = static_cast<SharedContext*>(ptr);
    uint8_t* buffer_start = static_cast<uint8_t*>(ptr) + sizeof(SharedContext);

    std::cout << "[Consumer] Старт. Знайдено payload: " << payload_size << " байт.\n";
    size_t tail = 0;

    for (int i = 1; i <= 5000; ++i) {
        sem_wait(&ctx->sem_used);

        // Обчислюємо позицію слота
        uint8_t* current_slot = buffer_start + (tail * slot_size);
        PacketHeader* header = reinterpret_cast<PacketHeader*>(current_slot);
        uint8_t* payload_ptr = current_slot + sizeof(PacketHeader);

        // ПЕРЕВІРКА ЦІЛІСНОСТІ
        uint32_t actual_checksum = calculate_checksum(payload_ptr, header->payload_size);
        if (actual_checksum != header->checksum) {
            std::cerr << "КРИТИЧНА ПОМИЛКА: Дані пошкоджено на пакеті " << header->seq_num << "!\n";
            return 1;
        }

        if (i % 1000 == 0) {
            std::cout << "[Consumer] Успішно перевірено пакет #" << header->seq_num 
                      << " | Час: " << header->timestamp_ns << " ns\n";
        }

        sem_post(&ctx->sem_free);
        tail = (tail + 1) % RING_CAPACITY;
    }

    std::cout << "[Consumer] Всі 5000 пакетів валідовані успішно!\n";
    
    sem_destroy(&ctx->sem_free);
    sem_destroy(&ctx->sem_used);
    shm_unlink(SHM_NAME);
    return 0;
}