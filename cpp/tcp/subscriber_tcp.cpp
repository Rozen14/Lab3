// ============================================================================
// subscriber_tcp.cpp — Suscriptor que recibe eventos deportivos por TCP
// ============================================================================
// Compilar: g++ -o subscriber_tcp subscriber_tcp.cpp -std=c++17
// Ejecutar: ./subscriber_tcp <tema>
// Ejemplo:  ./subscriber_tcp "EquipoA_vs_EquipoB"
// ============================================================================

#include <iostream>
#include <cstring>
#include <string>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

constexpr int BROKER_PORT = 8080;
constexpr int BUFFER_SIZE = 1024;
const char* BROKER_IP = "127.0.0.1";

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Uso: ./subscriber_tcp <tema>" << std::endl;
        return 1;
    }

    std::string tema = argv[1];

    // PASO 1: Crear socket TCP
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Error creando socket");
        return 1;
    }

    // PASO 2: Conectar al broker (three-way handshake)
    struct sockaddr_in broker_addr;
    memset(&broker_addr, 0, sizeof(broker_addr));
    broker_addr.sin_family = AF_INET;
    broker_addr.sin_port = htons(BROKER_PORT);
    inet_pton(AF_INET, BROKER_IP, &broker_addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&broker_addr, sizeof(broker_addr)) < 0) {
        perror("Error conectando al broker");
        close(sock);
        return 1;
    }

    // PASO 3: Enviar solicitud de suscripción
    std::string sub_msg = "SUB|" + tema;
    send(sock, sub_msg.c_str(), sub_msg.size(), 0);

    std::cout << "[SUBSCRIBER] Suscrito a '" << tema << "'. Esperando eventos..." << std::endl;
    std::cout << "-------------------------------------------" << std::endl;

    // PASO 4: Loop de recepción
    // recv() se bloquea hasta que lleguen datos.
    // En TCP, los datos llegan:
    //   - Completos (no se pierden bytes)
    //   - En orden (misma secuencia que el send() del broker)
    //   - Sin duplicados
    // Si el broker cierra la conexión, recv() retorna 0.
    char buffer[BUFFER_SIZE];
    while (true) {
        memset(buffer, 0, BUFFER_SIZE);
        int bytes = recv(sock, buffer, BUFFER_SIZE - 1, 0);

        if (bytes <= 0) {
            if (bytes == 0) {
                std::cout << "[SUBSCRIBER] Broker cerró la conexión." << std::endl;
            } else {
                perror("Error en recv");
            }
            break;
        }

        std::cout << "  >> " << buffer;
    }

    close(sock);
    return 0;
}