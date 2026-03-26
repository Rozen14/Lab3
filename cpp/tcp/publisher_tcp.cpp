// ============================================================================
// publisher_tcp.cpp — Publicador de eventos deportivos sobre TCP
// ============================================================================
// Compilar: g++ -o publisher_tcp publisher_tcp.cpp -std=c++17
// Ejecutar: ./publisher_tcp <tema>
// Ejemplo:  ./publisher_tcp "EquipoA_vs_EquipoB"
// ============================================================================

#include <iostream>
#include <cstring>
#include <string>
#include <vector>
#include <thread>
#include <chrono>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

constexpr int BROKER_PORT = 8080;
const char* BROKER_IP = "127.0.0.1";  // localhost

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Uso: ./publisher_tcp <tema>" << std::endl;
        std::cerr << "Ejemplo: ./publisher_tcp EquipoA_vs_EquipoB" << std::endl;
        return 1;
    }

    std::string tema = argv[1];

    // PASO 1: Crear socket TCP
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Error creando socket");
        return 1;
    }

    // PASO 2: Conectar al broker
    // connect() inicia el three-way handshake de TCP:
    //   Cliente → SYN → Broker
    //   Broker → SYN+ACK → Cliente
    //   Cliente → ACK → Broker
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

    std::cout << "[PUBLISHER] Conectado al broker. Tema: " << tema << std::endl;

    // PASO 3: Enviar mensajes de eventos deportivos
    // Formato: PUB|tema|contenido
    std::vector<std::string> eventos = {
        "Inicia el partido",
        "Falta de Equipo visitante al minuto 5",
        "Tarjeta roja para Orlando Flabio minuto 7",
        "Tiro de esquina para el local al minuto 12",
        "GOL del local al minuto 18",
        "Tarjeta amarilla #7 visitante al minuto 23",
        "Cambio: entra #9, sale #11 local al minuto 30",
        "Fin del primer tiempo",
        "Inicia segundo tiempo",
        "El tecnico parece estar dandole indicaciones al #10 del otro equipo minuto 52",
        "GOL del visitante al minuto 55",
        "Orlando Flabio parece estar rabioso con el arbitro Pipe Herrera minuto 69",
        "Penal para el local al minuto 70",
        "GOL de penal del local al minuto 71",
        "Tarjeta roja #3 visitante al minuto 80",
        "Se metio un hincha al partido y detuvieron la jugada minuto 83",
        "Fin del partido. Resultado: Local 2 - Visitante 1"
    };

    for (size_t i = 0; i < eventos.size(); i++) {
        std::string msg = "PUB|" + tema + "|" + eventos[i];

        // send() coloca los datos en el buffer del kernel.
        // TCP se encarga de segmentar, retransmitir, y garantizar orden.
        ssize_t sent = send(sock, msg.c_str(), msg.size(), 0);
        if (sent < 0) {
            perror("Error enviando mensaje");
            break;
        }

        std::cout << "[PUB → BROKER] " << eventos[i] << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    std::cout << "[PUBLISHER] Transmisión finalizada." << std::endl;
    close(sock);
    return 0;
}
