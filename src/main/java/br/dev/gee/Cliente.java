package br.dev.gee;

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
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        final Scanner scanner = new Scanner(System.in);
        final ArrayList<Servidor.NetworkInfo> availableServers = new ArrayList<>();
        final HashMap<String, Long> lastTimestamps = new HashMap<>();

        // Inicialização parcial
        for (int i = 1; i <= 3; i++)
            availableServers.add(new Servidor.NetworkInfo(
                    Servidor.readAddress(scanner, "Servidor " + i),
                    Servidor.readPort(scanner, "Servidor " + i)
            ));
        final Random rand = new Random();

        InetAddress serverAddress = null;
        int serverPort = -1;
        String option = "";

        Socket serverSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;

        while (!option.equals("EXIT")) {
            switch (option) {
                case "INIT":
                    // Inicialização completa (servidor escolhido de forma aleatória)
                    final Servidor.NetworkInfo randomServer = availableServers.get(rand.nextInt(3));
                    System.out.printf("Servidor selecionado aleatoriamente: %s\n", randomServer);
                    serverAddress = randomServer.address;
                    serverPort = randomServer.port;
                    if (serverSocket != null)
                        serverSocket.close();
                    serverSocket = new Socket(serverAddress, serverPort);
                    out = new ObjectOutputStream(serverSocket.getOutputStream());
                    in = new ObjectInputStream(serverSocket.getInputStream());
                    out.writeObject(new Mensagem(
                            Mensagem.Code.CLIENT_HERE,
                            null,
                            null,
                            -1
                    ));
                    break;
                case "PUT":
                    if (serverSocket == null || serverSocket.isClosed()) {
                        System.out.println("Execute INIT primeiro!");
                        break;
                    }
                    // Inicialização da requisição PUT
                    System.out.print("Insira a chave: ");
                    final String key = scanner.nextLine();
                    System.out.print("Insira o valor: ");
                    final String value = scanner.nextLine();
                    // Envio da requisição PUT
                    out.writeObject(new Mensagem(
                            Mensagem.Code.PUT,
                            key,
                            value,
                            -1
                    ));
                    Mensagem msg = (Mensagem) in.readObject();
                    if (msg.code == Mensagem.Code.PUT_OK) {
                        // Requisição foi um sucesso
                        System.out.printf(
                                "PUT_OK key: %s value %s timestamp %d realizada no servidor %s:%d\n",
                                msg.key,
                                value,
                                msg.timestamp,
                                serverAddress,
                                serverPort
                        );
                        lastTimestamps.put(msg.key, msg.timestamp);
                    }
                    break;
                case "GET":
                    if (serverSocket == null || serverSocket.isClosed()) {
                        System.out.println("Execute INIT primeiro!");
                        break;
                    }
                    // Requisição GET
                    System.out.print("Insira a chave: ");
                    final String key2 = scanner.nextLine();
                    final long timestamp = lastTimestamps.getOrDefault(key2, 0L);
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
                                "GET key: %s value: %s obtido do servidor %s:%d, meu timestamp %d e do servidor %d\n",
                                msg2.key,
                                msg2.value,
                                serverAddress,
                                serverPort,
                                timestamp,
                                msg2.timestamp
                        );
                    }
                default:
            }
            // Menu interativo
            System.out.print("Insira uma opção (INIT, PUT, GET): ");
            option = scanner.nextLine().toUpperCase();
        }

        scanner.close();
    }
}
