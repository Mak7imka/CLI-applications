#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <csignal>
#include <thread>
#include <iomanip>
#include "common.hpp"

std::atomic<bool> is_running{true};
std::atomic<bool> is_paused{false};

std::atomic<uint64_t> total_packets{0};
std::atomic<uint64_t> total_bytes{0};

void handle_signal(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        is_running = false;
    }
}

int main() {
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    std::thread input_thread([]() {
        while (is_running) {
            if (std::cin.get() != EOF) {
                is_paused = !is_paused;
                std::cout << (is_paused ? "\n[CONSUMER ПАУЗА]\n" : "\n[CONSUMER ВІДНОВЛЕНО]\n");
            }
        }
    });
    input_thread.detach();

    int fd = shm_open(SHM_NAME, O_RDWR, 0666);
    if (fd == -1) { 
        std::cerr << "Помилка: Запусти Producer спочатку!\n"; 
        return 1; 
    }

    void* temp_ptr = mmap(0, sizeof(SharedContext), PROT_READ, MAP_SHARED, fd, 0);
    if (temp_ptr == MAP_FAILED) { perror("mmap"); return 1; }
    SharedContext* temp_ctx = static_cast<SharedContext*>(temp_ptr);
    uint32_t payload_size = temp_ctx->configured_payload_size;
    munmap(temp_ptr, sizeof(SharedContext));

    size_t slot_size = sizeof(PacketHeader) + payload_size;
    size_t total_shm_size = sizeof(SharedContext) + (RING_CAPACITY * slot_size);

    void* ptr = mmap(0, total_shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); return 1; }
    SharedContext* ctx = static_cast<SharedContext*>(ptr);
    uint8_t* buffer_start = static_cast<uint8_t*>(ptr) + sizeof(SharedContext);

    // ПІДКЛЮЧАЄМОСЬ ДО ІСНУЮЧИХ СЕМАФОРІВ
    sem_t* sem_free = sem_open(SEM_FREE_NAME, 0);
    sem_t* sem_used = sem_open(SEM_USED_NAME, 0);

    if (sem_free == SEM_FAILED || sem_used == SEM_FAILED) {
        std::cerr << "Помилка підключення до семафорів. Перезапусти Producer!\n";
        return 1;
    }

    std::cout << "[Consumer] Старт.\nНатисни ENTER для Паузи. Ctrl+C для виходу.\n\n";

    std::thread stats_thread([&]() {
        uint64_t last_packets = 0;
        uint64_t last_bytes = 0;
        while (is_running) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            if (is_paused) continue;

            uint64_t cur_packets = total_packets.load();
            uint64_t cur_bytes = total_bytes.load();

            double mb_per_sec = (cur_bytes - last_bytes) / (1024.0 * 1024.0);
            
            std::cout << "[Статистика] Пакетів: " << cur_packets 
                      << " | Швидкість: " << (cur_packets - last_packets) << " пак/с"
                      << " | Пропускна здатність: " << std::fixed << std::setprecision(2) << mb_per_sec << " MB/s\n";
            
            last_packets = cur_packets;
            last_bytes = cur_bytes;
        }
    });

    size_t tail = 0;

    while (is_running) {
        while (is_paused && is_running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (!is_running) break;

        sem_wait(sem_used); // Чекаємо на нові дані через іменований семафор

        if (!ctx->producer_active.load()) {
            int used_val;
            sem_getvalue(sem_used, &used_val);
            if (used_val == 0) {
                std::cout << "\n[Consumer] Producer завершив роботу. Вихід.\n";
                break;
            }
        }

        uint8_t* current_slot = buffer_start + (tail * slot_size);
        PacketHeader* header = reinterpret_cast<PacketHeader*>(current_slot);
        uint8_t* payload_ptr = current_slot + sizeof(PacketHeader);

        uint32_t actual_checksum = calculate_checksum(payload_ptr, header->payload_size);
        if (actual_checksum != header->checksum) {
            std::cerr << "КРИТИЧНА ПОМИЛКА: Дані пошкоджено!\n";
            is_running = false; break;
        }

        total_packets++;
        total_bytes += header->payload_size;

        sem_post(sem_free); // Повідомляємо про звільнення слота
        tail = (tail + 1) % RING_CAPACITY;
    }

    is_running = false;
    stats_thread.join(); 
    
    // ОЧИЩЕННЯ РЕСУРСІВ ОС
    sem_close(sem_free);
    sem_close(sem_used);
    sem_unlink(SEM_FREE_NAME);
    sem_unlink(SEM_USED_NAME);
    shm_unlink(SHM_NAME);
    
    return 0;
}
