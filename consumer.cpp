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

    void* ptr = mmap(0, sizeof(SharedContext), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); return 1; }

    SharedContext* ctx = static_cast<SharedContext*>(ptr);

    std::cout << "[Consumer] Очікування даних...\n";
    
    size_t tail = 0;

    for (int i = 1; i <= 5000; ++i) {
        sem_wait(&ctx->sem_used);

        // Читаємо повідомлення кожен 1000-й пакет для перевірки
        if (i % 1000 == 0) {
            std::cout << "[Consumer] Прочитано: " << ctx->messages[tail] << "\n";
        }

        sem_post(&ctx->sem_free);

        tail = (tail + 1) % RING_CAPACITY;
    }

    std::cout << "[Consumer] Всі 5000 пакетів успішно прочитано!\n";
    shm_unlink(SHM_NAME);
    return 0;
}
