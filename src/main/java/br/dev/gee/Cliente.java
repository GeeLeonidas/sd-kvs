package br.dev.gee;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Scanner;

public class Cliente {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        final Scanner scanner = new Scanner(System.in);

        InetAddress serverAddress = null;
        int serverPort = -1;
        String option = "";

        Socket serverSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;

        while (!option.equals("EXIT")) {
            switch (option) {
                case "INIT":
                    serverAddress = Servidor.readAddress(scanner, "Servidor");
                    serverPort = Servidor.readPort(scanner, "Servidor");
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
                    System.out.print("Insira a chave: ");
                    final String key = scanner.nextLine();
                    System.out.print("Insira o valor: ");
                    final String value = scanner.nextLine();
                    out.writeObject(new Mensagem(
                            Mensagem.Code.PUT,
                            key,
                            value,
                            -1
                    ));
                    Mensagem msg = (Mensagem) in.readObject();
                    if (msg.code == Mensagem.Code.PUT_OK)
                        System.out.printf(
                                "PUT_OK key: %s value %s timestamp %d realizada no servidor %s:%d\n",
                                msg.key,
                                value,
                                msg.timestamp,
                                serverAddress,
                                serverPort
                        );
                    break;
                case "GET":
                    if (serverSocket == null || serverSocket.isClosed()) {
                        System.out.println("Execute INIT primeiro!");
                        break;
                    }
                    System.out.print("Insira a chave: ");
                    final String key2 = scanner.nextLine();
                    System.out.print("Insira o timestamp: ");
                    final long timestamp = Long.parseLong(scanner.nextLine());
                    out.writeObject(new Mensagem(
                            Mensagem.Code.GET,
                            key2,
                            null,
                            timestamp
                    ));
                    Mensagem msg2 = (Mensagem) in.readObject();
                    if (msg2.code == Mensagem.Code.GET)
                        System.out.printf(
                                "GET key: %s value: %s obtido do servidor %s:%d, meu timestamp %d e do servidor %d\n",
                                msg2.key,
                                msg2.value,
                                serverAddress,
                                serverPort,
                                timestamp,
                                msg2.timestamp
                        );
                default:
            }
            System.out.print("Insira uma opção (INIT, PUT, GET): ");
            option = scanner.nextLine().toUpperCase();
        }

        scanner.close();
    }
}
