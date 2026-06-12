#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h> // Потрібно для констант режимів
#include <unistd.h>
#include <cstring>
#include <chrono>
#include <vector>
#include <thread>
#include <csignal>
#include "common.hpp"

// Локальні прапорці керування станом
std::atomic<bool> is_running{true};
std::atomic<bool> is_paused{false};

void handle_signal(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        is_running = false;
    }
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Використання: ./producer <розмір_payload_у_байтах>\n";
        return 1;
    }
    uint32_t payload_size = std::stoul(argv[1]);

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    std::thread input_thread([]() {
        while (is_running) {
            if (std::cin.get() != EOF) {
                is_paused = !is_paused;
                std::cout << (is_paused ? "\n[PRODUCER ПАУЗА]\n" : "\n[PRODUCER ВІДНОВЛЕНО]\n");
            }
        }
    });
    input_thread.detach();

    // ОЧИЩЕННЯ ПЕРЕД СТАРТОМ: знищуємо "привидів" від минулих запусків
    shm_unlink(SHM_NAME);
    sem_unlink(SEM_FREE_NAME);
    sem_unlink(SEM_USED_NAME);

    size_t slot_size = sizeof(PacketHeader) + payload_size;
    size_t total_shm_size = sizeof(SharedContext) + (RING_CAPACITY * slot_size);

    int fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
    if (fd == -1) { perror("shm_open"); return 1; }

    // Перевіряємо помилку ftruncate
    if (ftruncate(fd, total_shm_size) == -1) { perror("ftruncate"); return 1; }
    
    void* ptr = mmap(0, total_shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); return 1; }
    
    SharedContext* ctx = static_cast<SharedContext*>(ptr);
    uint8_t* buffer_start = static_cast<uint8_t*>(ptr) + sizeof(SharedContext);

    // СТВОРЕННЯ ІМЕНОВАНИХ СЕМАФОРІВ (Кросплатформенно)
    sem_t* sem_free = sem_open(SEM_FREE_NAME, O_CREAT, 0666, RING_CAPACITY);
    sem_t* sem_used = sem_open(SEM_USED_NAME, O_CREAT, 0666, 0);

    if (sem_free == SEM_FAILED || sem_used == SEM_FAILED) {
        perror("sem_open"); return 1;
    }

    ctx->configured_payload_size = payload_size;
    ctx->producer_active = true;

    std::vector<uint8_t> dummy_data(payload_size, 0xAB);
    size_t head = 0;
    uint64_t seq_num = 1;

    std::cout << "[Producer] Старт. Payload: " << payload_size << " байт.\n";
    std::cout << "[Producer] Натисніть Enter для паузи/відновлення...\n";

    while (is_running) {
        while (is_paused && is_running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (!is_running) break;

        sem_wait(sem_free); // Тепер ми передаємо сам вказівник sem_free

        uint8_t* current_slot = buffer_start + (head * slot_size);
        PacketHeader* header = reinterpret_cast<PacketHeader*>(current_slot);
        uint8_t* payload_ptr = current_slot + sizeof(PacketHeader);

        header->seq_num = seq_num++;
        header->timestamp_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::high_resolution_clock::now().time_since_epoch()).count();
        header->payload_size = payload_size;
        
        std::memcpy(payload_ptr, dummy_data.data(), payload_size);
        header->checksum = calculate_checksum(payload_ptr, payload_size);

        sem_post(sem_used); // Повідомляємо Consumer-а через іменований семафор
        head = (head + 1) % RING_CAPACITY;
    }

    std::cout << "\n[Producer] Завершення роботи...\n";
    ctx->producer_active = false;
    sem_post(sem_used); // Розблоковуємо Consumer, якщо він чекає

    // Відключаємо семафори
    sem_close(sem_free);
    sem_close(sem_used);

    return 0;
}