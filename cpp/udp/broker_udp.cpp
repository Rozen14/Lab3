// ============================================================================
// broker_udp.cpp — Broker central del sistema pub-sub sobre UDP
// ============================================================================
// Compilar: g++ -o broker_udp broker_udp.cpp -std=c++17
// Ejecutar: ./broker_udp
//
// DIFERENCIA CLAVE CON TCP:
//   - NO hay connect(), listen(), ni accept(). No hay conexiones.
//   - Un solo socket recibe TODO (suscripciones y publicaciones).
//   - Usa recvfrom() que te da la dirección del remitente.
//   - Usa sendto() para enviar a una dirección específica.
//   - No hay garantía de entrega, orden, ni detección de desconexión.
// ============================================================================

#include <iostream>
#include <cstring>
#include <string>
#include <vector>
#include <map>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

constexpr int PORT = 9090;
constexpr int BUFFER_SIZE = 1024;

// Como no hay conexiones persistentes, necesitamos guardar la dirección
// completa (IP + puerto) de cada suscriptor.
struct UdpSubscriber {
    struct sockaddr_in addr;

    bool operator==(const UdpSubscriber& other) const {
        return addr.sin_addr.s_addr == other.addr.sin_addr.s_addr &&
               addr.sin_port == other.addr.sin_port;
    }
};

int main() {
    // PASO 1: Crear socket UDP
    // SOCK_DGRAM = datagramas independientes, sin conexión
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        perror("Error creando socket UDP");
        return 1;
    }

    // PASO 2: bind() — asociar a un puerto
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    if (bind(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Error en bind");
        close(sock);
        return 1;
    }

    std::cout << "[BROKER UDP] Escuchando en puerto " << PORT << std::endl;

    // NO hay listen() ni accept() — UDP no establece conexiones

    // tema → lista de suscriptores (dirección IP + puerto)
    std::map<std::string, std::vector<UdpSubscriber>> subscriptions;

    // PASO 3: Loop principal
    // No necesitamos select() porque tenemos un solo socket
    char buffer[BUFFER_SIZE];

    while (true) {
        struct sockaddr_in sender_addr;
        socklen_t addr_len = sizeof(sender_addr);
        memset(buffer, 0, BUFFER_SIZE);

        // recvfrom() = versión UDP de recv()
        // Además de los datos, nos da la dirección del remitente.
        // Cada llamada recibe UN datagrama completo.
        int bytes = recvfrom(sock, buffer, BUFFER_SIZE - 1, 0,
                             (struct sockaddr*)&sender_addr, &addr_len);

        if (bytes < 0) {
            perror("Error en recvfrom");
            continue;
        }

        std::string msg(buffer, bytes);
        std::cout << "[BROKER] Recibido de " << inet_ntoa(sender_addr.sin_addr)
                  << ":" << ntohs(sender_addr.sin_port)
                  << " -> " << msg << std::endl;

        // Parsear protocolo
        char temp[BUFFER_SIZE];
        strncpy(temp, buffer, BUFFER_SIZE);
        char* cmd = strtok(temp, "|");
        char* tema = strtok(nullptr, "|");

        if (cmd == nullptr || tema == nullptr) continue;

        if (strcmp(cmd, "SUB") == 0) {
            UdpSubscriber sub;
            sub.addr = sender_addr;

            // Verificar que no esté ya suscrito
            auto& subs = subscriptions[tema];
            bool already = false;
            for (const auto& s : subs) {
                if (s == sub) { already = true; break; }
            }

            if (!already) {
                subs.push_back(sub);
                std::cout << "[BROKER] Nuevo suscriptor a '" << tema << "' desde "
                          << inet_ntoa(sender_addr.sin_addr) << ":"
                          << ntohs(sender_addr.sin_port) << std::endl;
            }

            // Confirmar suscripción
            std::string ack = "OK|Suscrito a " + std::string(tema);
            sendto(sock, ack.c_str(), ack.size(), 0,
                   (struct sockaddr*)&sender_addr, sizeof(sender_addr));

        } else if (strcmp(cmd, "PUB") == 0) {
            char* mensaje = strtok(nullptr, "");
            if (mensaje == nullptr) continue;

            std::string to_send = "[" + std::string(tema) + "] " + std::string(mensaje);
            std::cout << "[BROKER] Distribuyendo a tema '" << tema
                      << "': " << mensaje << std::endl;

            // sendto() = versión UDP de send()
            // NO hay garantía de que llegue. Si se pierde, se pierde.
            for (const auto& sub : subscriptions[tema]) {
                sendto(sock, to_send.c_str(), to_send.size(), 0,
                       (struct sockaddr*)&sub.addr, sizeof(sub.addr));
            }
        }
    }

    close(sock);
    return 0;
}