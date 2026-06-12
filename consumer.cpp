#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

int main() {
    const char* shm_name = "/shm";
    const size_t size = 128;

    int fd = shm_open(shm_name, O_RDONLY, 0666);
    if (fd == -1) { 
        std::cerr << "Помилка: пам'ять не знайдено.\n"; 
        return 1; 
    }

    void* ptr = mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); return 1; }

    char* data = static_cast<char*>(ptr);
    std::cout << "[Consumer] Отримано дані: " << data << "\n";

    // 4. Очищення ресурсів ОС
    shm_unlink(shm_name);

    return 0;
}
