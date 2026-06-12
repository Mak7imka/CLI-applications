#include <iostream>
#include <fcntl.h>      // Для O_CREAT, O_RDWR
#include <sys/mman.h>   // Для shm_open, mmap
#include <unistd.h>     // Для ftruncate
#include <cstring>      // Для std::memcpy

int main() {
    const char* shm_name = "/shm";
    const size_t size = 128;

    // 1. Створюємо (O_CREAT) і відкриваємо спільну пам'ять
    int fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
    if (fd == -1) { perror("shm_open"); return 1; }

    ftruncate(fd, size);

    void* ptr = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); return 1; }

    const char* payload = "Hello from Producer! This is Shared Memory.";
    std::memcpy(ptr, payload, std::strlen(payload) + 1);

    std::cout << "[Producer] Пакети даних розміщено у спільній пам'яті.\n";
    std::cout << "Натисни Enter, щоб завершити роботу Producer'а...\n";
    std::cin.get();

    return 0;
}
