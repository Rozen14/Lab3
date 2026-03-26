// ============================================================================
// broker_tcp.cpp — Broker central del sistema pub-sub sobre TCP
// ============================================================================
// Compilar: g++ -o broker_tcp broker_tcp.cpp -std=c++17
// Ejecutar: ./broker_tcp
//
// El broker escucha en el puerto 8080. Cuando un cliente se conecta:
//   - Si envía "SUB|tema", se registra como suscriptor de ese tema.
//   - Si envía "PUB|tema|mensaje", el broker reenvía el mensaje
//     a todos los suscriptores registrados en ese tema.
//
// Usa select() para manejar múltiples conexiones sin hilos.
// ============================================================================

#include <iostream>
#include <cstring>       // memset, strtok
#include <string>
#include <vector>
#include <map>
#include <set>
#include <algorithm>

// --- Headers POSIX para sockets ---
#include <sys/socket.h>  // socket(), bind(), listen(), accept(), send(), recv()
#include <netinet/in.h>  // struct sockaddr_in, htons(), INADDR_ANY
#include <arpa/inet.h>   // inet_ntoa()
#include <unistd.h>      // close()
#include <sys/select.h>  // select(), fd_set, FD_SET, FD_CLR, FD_ISSET

constexpr int PORT = 8080;
constexpr int BUFFER_SIZE = 1024;
constexpr int MAX_PENDING = 10;  // Backlog para listen()

int main() {
    // ==================================================================
    // PASO 1: Crear el socket TCP
    // ==================================================================
    // socket(dominio, tipo, protocolo)
    //   AF_INET     = IPv4
    //   SOCK_STREAM = TCP (flujo confiable, orientado a conexión)
    //   0           = protocolo por defecto (TCP para SOCK_STREAM)
    // Retorna un file descriptor (entero) que identifica este socket.
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("Error creando socket");
        return 1;
    }

    // Permitir reutilizar el puerto inmediatamente después de cerrar
    // (evita el error "Address already in use" al reiniciar rápido)
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // ==================================================================
    // PASO 2: Asociar el socket a una dirección IP y puerto (bind)
    // ==================================================================
    // sockaddr_in es la estructura que define la dirección para IPv4.
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;         // IPv4
    server_addr.sin_addr.s_addr = INADDR_ANY; // Escuchar en todas las interfaces
    server_addr.sin_port = htons(PORT);        // Puerto en Network Byte Order

    // bind() asocia el socket al puerto. Sin esto, el SO no sabe
    // a qué puerto dirigir los paquetes entrantes.
    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Error en bind");
        close(server_fd);
        return 1;
    }

    // ==================================================================
    // PASO 3: Poner el socket en modo escucha (listen)
    // ==================================================================
    // listen() marca el socket como pasivo — acepta conexiones entrantes.
    // MAX_PENDING = cuántas conexiones pueden esperar en cola antes de
    // que el SO las rechace.
    if (listen(server_fd, MAX_PENDING) < 0) {
        perror("Error en listen");
        close(server_fd);
        return 1;
    }

    std::cout << "[BROKER TCP] Escuchando en puerto " << PORT << std::endl;

    // ==================================================================
    // Estructuras de datos del broker
    // ==================================================================
    // tema → conjunto de file descriptors suscritos a ese tema
    std::map<std::string, std::set<int>> subscriptions;

    // Todos los file descriptors activos (para select)
    std::set<int> all_fds;

    // ==================================================================
    // PASO 4: Loop principal con select()
    // ==================================================================
    // select() permite monitorear múltiples sockets simultáneamente
    // sin crear hilos. Bloquea hasta que algún fd tenga datos listos.
    while (true) {
        // Construir el fd_set para select()
        fd_set read_fds;
        FD_ZERO(&read_fds);                  // Limpiar el conjunto
        FD_SET(server_fd, &read_fds);         // Siempre monitorear el server
        int max_fd = server_fd;

        for (int fd : all_fds) {
            FD_SET(fd, &read_fds);
            if (fd > max_fd) max_fd = fd;
        }

        // select(nfds, readfds, writefds, exceptfds, timeout)
        // nfds = max_fd + 1 (rango de fds a revisar)
        // NULL en timeout = bloquear indefinidamente
        int activity = select(max_fd + 1, &read_fds, nullptr, nullptr, nullptr);
        if (activity < 0) {
            perror("Error en select");
            break;
        }

        // ¿Hay una nueva conexión entrante en el socket servidor?
        if (FD_ISSET(server_fd, &read_fds)) {
            // accept() extrae la primera conexión de la cola de listen()
            // y crea un NUEVO socket para esta conexión específica.
            struct sockaddr_in client_addr;
            socklen_t addr_len = sizeof(client_addr);
            int new_fd = accept(server_fd, (struct sockaddr*)&client_addr, &addr_len);

            if (new_fd >= 0) {
                all_fds.insert(new_fd);
                std::cout << "[BROKER] Nueva conexión: fd=" << new_fd
                          << " desde " << inet_ntoa(client_addr.sin_addr)
                          << ":" << ntohs(client_addr.sin_port) << std::endl;
            }
        }

        // Revisar cada cliente conectado
        std::vector<int> to_remove;
        for (int fd : all_fds) {
            if (!FD_ISSET(fd, &read_fds)) continue;

            char buffer[BUFFER_SIZE];
            memset(buffer, 0, BUFFER_SIZE);

            // recv(fd, buffer, tamaño, flags)
            // Retorna: >0 = bytes recibidos, 0 = cliente cerró conexión, <0 = error
            int bytes = recv(fd, buffer, BUFFER_SIZE - 1, 0);

            if (bytes <= 0) {
                // Cliente desconectado — limpiar de todas las suscripciones
                std::cout << "[BROKER] Desconectado: fd=" << fd << std::endl;
                close(fd);
                to_remove.push_back(fd);
                for (auto& [tema, subs] : subscriptions) {
                    subs.erase(fd);
                }
                continue;
            }

            std::string msg(buffer);
            std::cout << "[BROKER] Recibido de fd=" << fd << ": " << msg << std::endl;

            // Parsear el protocolo: "SUB|tema" o "PUB|tema|mensaje"
            char* cmd = strtok(buffer, "|");
            char* tema = strtok(nullptr, "|");

            if (cmd == nullptr || tema == nullptr) continue;

            if (strcmp(cmd, "SUB") == 0) {
                // Registrar suscripción
                subscriptions[tema].insert(fd);
                std::cout << "[BROKER] fd=" << fd << " suscrito a '" << tema << "'" << std::endl;

                // Confirmar al suscriptor
                std::string ack = "OK|Suscrito a " + std::string(tema) + "\n";
                send(fd, ack.c_str(), ack.size(), 0);

            } else if (strcmp(cmd, "PUB") == 0) {
                char* mensaje = strtok(nullptr, "");  // El resto es el mensaje
                if (mensaje == nullptr) continue;

                std::string to_send = "[" + std::string(tema) + "] " + std::string(mensaje) + "\n";
                std::cout << "[BROKER] Distribuyendo a tema '" << tema
                          << "': " << mensaje << std::endl;

                // send(fd, datos, tamaño, flags)
                // En TCP, send() garantiza que los datos llegan completos y en orden.
                for (int sub_fd : subscriptions[tema]) {
                    send(sub_fd, to_send.c_str(), to_send.size(), 0);
                }
            }
        }

        // Remover fds desconectados
        for (int fd : to_remove) {
            all_fds.erase(fd);
        }
    }

    close(server_fd);
    return 0;
}