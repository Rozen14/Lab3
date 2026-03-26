// ============================================================================
// subscriber_udp.cpp — Suscriptor que recibe eventos deportivos por UDP
// ============================================================================
// Compilar: g++ -o subscriber_udp subscriber_udp.cpp -std=c++17
// Ejecutar: ./subscriber_udp <tema>
//
// NOTA: El subscriber UDP hace bind() a un puerto local para que el broker
// sepa dónde enviarle los mensajes de vuelta.
// ============================================================================

#include <iostream>
#include <cstring>
#include <string>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

constexpr int BROKER_PORT = 9090;
constexpr int BUFFER_SIZE = 1024;
const char* BROKER_IP = "127.0.0.1";

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Uso: ./subscriber_udp <tema>" << std::endl;
        return 1;
    }

    std::string tema = argv[1];

    // Crear socket UDP
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        perror("Error creando socket");
        return 1;
    }

    // Bind a puerto 0 = el SO asigna un puerto libre
    // Fija el puerto efímero para que el broker sepa dónde responder
    struct sockaddr_in my_addr;
    memset(&my_addr, 0, sizeof(my_addr));
    my_addr.sin_family = AF_INET;
    my_addr.sin_addr.s_addr = INADDR_ANY;
    my_addr.sin_port = htons(0);  // Puerto automático

    if (bind(sock, (struct sockaddr*)&my_addr, sizeof(my_addr)) < 0) {
        perror("Error en bind");
        close(sock);
        return 1;
    }

    // Dirección del broker
    struct sockaddr_in broker_addr;
    memset(&broker_addr, 0, sizeof(broker_addr));
    broker_addr.sin_family = AF_INET;
    broker_addr.sin_port = htons(BROKER_PORT);
    inet_pton(AF_INET, BROKER_IP, &broker_addr.sin_addr);

    // Enviar solicitud de suscripción
    std::string sub_msg = "SUB|" + tema;
    sendto(sock, sub_msg.c_str(), sub_msg.size(), 0,
           (struct sockaddr*)&broker_addr, sizeof(broker_addr));

    std::cout << "[SUBSCRIBER UDP] Suscrito a '" << tema << "'. Esperando eventos..." << std::endl;
    std::cout << "-------------------------------------------" << std::endl;

    // Loop de recepción
    // DIFERENCIAS CON TCP recv():
    //   - Si un datagrama se pierde, recvfrom() se queda bloqueado esperando
    //   - Los datagramas pueden llegar DESORDENADOS
    //   - No hay forma de saber si perdiste un mensaje
    //   - Si el broker se cae, recvfrom() se queda colgado para siempre
    //     (TCP recv() retorna 0 en ese caso)
    char buffer[BUFFER_SIZE];
    while (true) {
        memset(buffer, 0, BUFFER_SIZE);
        struct sockaddr_in from_addr;
        socklen_t from_len = sizeof(from_addr);

        int bytes = recvfrom(sock, buffer, BUFFER_SIZE - 1, 0,
                             (struct sockaddr*)&from_addr, &from_len);

        if (bytes < 0) {
            perror("Error en recvfrom");
            continue;
        }

        std::cout << "  >> " << buffer << std::endl;
    }

    close(sock);
    return 0;
}