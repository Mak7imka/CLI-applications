#include <iostream>
#include <fcntl.h>      // Для O_CREAT, O_RDWR
#include <sys/mman.h>   // Для shm_open, mmap
#include <unistd.h>     // Для ftruncate
#include <cstring>      // Для std::memcpy
#include <string>        // Для std::string
#include "common.hpp"

int main() {
    int fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
    if (fd == -1) { perror("shm_open"); return 1; }
    
    ftruncate(fd, sizeof(SharedContext));
    
    void* ptr = mmap(0, sizeof(SharedContext), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); return 1; }
    
    SharedContext* ctx = static_cast<SharedContext*>(ptr);

    // Ініціалізація семафорів
    sem_init(&ctx->sem_free, 1, RING_CAPACITY);
    sem_init(&ctx->sem_used, 1, 0);

    std::cout << "[Producer] Запуск передачі 5000 пакетів...\n";
    
    size_t head = 0;

    for (int i = 1; i <= 5000; ++i) {
        sem_wait(&ctx->sem_free); 

        std::string text = "Пакет даних номер " + std::to_string(i);
        std::strncpy(ctx->messages[head], text.c_str(), MESSAGE_SIZE);

        sem_post(&ctx->sem_used); 

        head = (head + 1) % RING_CAPACITY;
    }

    std::cout << "[Producer] Передачу завершено.\n";
    return 0;
}
