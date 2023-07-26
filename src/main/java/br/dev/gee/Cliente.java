package br.dev.gee;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;

public class Cliente {
    public static void main(String[] args) {
        final Scanner scanner = new Scanner(System.in);
        final ArrayList<Servidor.NetworkInfo> availableServers = new ArrayList<>();
        final HashMap<String, Long> lastTimestamps = new HashMap<>();

        final Random rand = new Random();

        String option = "";

        while (!option.equals("EXIT")) {
            switch (option) {
                case "INIT":
                    // Inicialização do grupo de servidores
                    availableServers.clear();
                    for (int i = 1; i <= 3; i++)
                        availableServers.add(new Servidor.NetworkInfo(
                                Servidor.readAddress(scanner, "Servidor " + i),
                                Servidor.readPort(scanner, "Servidor " + i)
                        ));
                    break;
                case "PUT":
                    if (availableServers.isEmpty()) {
                        System.out.println("Execute INIT primeiro!");
                        break;
                    }
                    // Inicialização da requisição PUT
                    System.out.print("Insira a chave: ");
                    final String key = scanner.nextLine();
                    System.out.print("Insira o valor: ");
                    final String value = scanner.nextLine();
                    // Execução em outra thread
                    final Servidor.NetworkInfo randomServer = availableServers.get(rand.nextInt(3));
                    System.out.printf("Servidor selecionado aleatoriamente: %s\n", randomServer);
                    new Thread(() -> {
                        final InetAddress serverAddress = randomServer.address;
                        final int serverPort = randomServer.port;
                        try (
                                Socket serverSocket = new Socket(serverAddress, serverPort);
                                ObjectOutputStream out = new ObjectOutputStream(serverSocket.getOutputStream());
                                ObjectInputStream in = new ObjectInputStream(serverSocket.getInputStream())
                        ) {
                            // Sinalização do cliente
                            out.writeObject(new Mensagem(
                                    Mensagem.Code.CLIENT_HERE,
                                    null,
                                    null,
                                    -1
                            ));
                            // Envio da requisição PUT
                            out.writeObject(new Mensagem(
                                    Mensagem.Code.PUT,
                                    key,
                                    value,
                                    -1
                            ));
                            // Atualiza o timestamp lógico espelhando o servidor
                            lastTimestamps.put(key, lastTimestamps.getOrDefault(key, 0L) + 1L);
                            // Aguardo da resposta do servidor
                            Mensagem msg = (Mensagem) in.readObject();
                            if (msg.code == Mensagem.Code.PUT_OK) {
                                // Requisição foi um sucesso
                                System.out.printf(
                                        "\nPUT_OK key: %s value %s timestamp %d realizada no servidor %s:%d\n",
                                        msg.key,
                                        value,
                                        msg.timestamp,
                                        serverAddress,
                                        serverPort
                                );
                                lastTimestamps.put(msg.key, msg.timestamp);
                            }
                        } catch (IOException | ClassNotFoundException exception) {
                            if (!(exception instanceof EOFException))
                                throw new RuntimeException(exception);
                        }
                    }).start();
                    break;
                case "GET":
                    if (availableServers.isEmpty()) {
                        System.out.println("Execute INIT primeiro!");
                        break;
                    }
                    // Requisição GET
                    System.out.print("Insira a chave: ");
                    final String key2 = scanner.nextLine();
                    final long timestamp = lastTimestamps.getOrDefault(key2, 0L);
                    // Execução em outra thread
                    final Servidor.NetworkInfo randomServer2 = availableServers.get(rand.nextInt(3));
                    System.out.printf("Servidor selecionado aleatoriamente: %s\n", randomServer2);
                    new Thread(() -> {
                        final InetAddress serverAddress = randomServer2.address;
                        final int serverPort = randomServer2.port;
                        try (
                                Socket serverSocket = new Socket(serverAddress, serverPort);
                                ObjectOutputStream out = new ObjectOutputStream(serverSocket.getOutputStream());
                                ObjectInputStream in = new ObjectInputStream(serverSocket.getInputStream())
                        ) {
                            // Sinalização do cliente
                            out.writeObject(new Mensagem(
                                    Mensagem.Code.CLIENT_HERE,
                                    null,
                                    null,
                                    -1
                            ));
                            // Envio da requisição GET
                            out.writeObject(new Mensagem(
                                    Mensagem.Code.GET,
                                    key2,
                                    null,
                                    timestamp
                            ));
                            Mensagem msg2 = (Mensagem) in.readObject();
                            if (msg2.code == Mensagem.Code.GET) {
                                if (msg2.value != null)
                                    lastTimestamps.put(msg2.key, msg2.timestamp);
                                System.out.printf(
                                        "\nGET key: %s value: %s obtido do servidor %s:%d, meu timestamp %d e do servidor %d\n",
                                        msg2.key,
                                        msg2.value,
                                        serverAddress,
                                        serverPort,
                                        timestamp,
                                        msg2.timestamp
                                );
                            }
                        } catch (IOException | ClassNotFoundException exception) {
                            if (!(exception instanceof EOFException))
                                throw new RuntimeException(exception);
                        }
                    }).start();
                default:
            }
            // Menu interativo
            System.out.print("Insira uma opção (INIT, PUT, GET): ");
            option = scanner.nextLine().toUpperCase();
        }

        scanner.close();
    }
}
