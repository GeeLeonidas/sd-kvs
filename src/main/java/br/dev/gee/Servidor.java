package br.dev.gee;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Scanner;

public class Servidor {
    public static class TimestampedValue<T> {
        public final long timestamp;
        public final T value;

        public TimestampedValue(T value) {
            this.timestamp = Instant.now().toEpochMilli();
            this.value = value;
        }
    }

    public static final InetAddress DEFAULT_ADDRESS;
    static {
        try {
            DEFAULT_ADDRESS = InetAddress.getByName("localhost");
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
    public static final int DEFAULT_PORT = 10097;

    public static InetAddress readAddress(Scanner scanner, String caller) {
        while (true) {
            System.out.printf("Insira o endereço do %s (padrão %s): ", caller, DEFAULT_ADDRESS.toString());
            final String input = scanner.nextLine();
            if (input.isEmpty())
                return DEFAULT_ADDRESS;
            try {
                return InetAddress.getByName(input);
            } catch (UnknownHostException ignored) {}
        }
    }

    public static int readPort(Scanner scanner, String caller) {
        while (true) {
            System.out.printf("Insira a porta do %s (padrão %s): ", caller, DEFAULT_PORT);
            final String input = scanner.nextLine();
            // Portas válidas (Fonte: https://ihateregex.io/expr/port)
            if (input.matches("^((6553[0-5])|(655[0-2][0-9])|(65[0-4][0-9]{2})|(6[0-4][0-9]{3})|([1-5][0-9]{4})|([0-5]{0,5})|([0-9]{1,4}))$"))
                return Integer.parseInt(input);
        }
    }

    public static void main(String[] args) throws IOException {
        final Scanner scanner = new Scanner(System.in);
        final InetAddress selfAddress = readAddress(scanner, "Host");
        final int selfPort = readPort(scanner, "Host");
        final InetAddress leaderAddress = readAddress(scanner, "Líder");
        final int leaderPort = readPort(scanner, "Líder");
        scanner.close();

        final ServerSocket serverSocket = new ServerSocket(selfPort, -1, selfAddress);
        final HashMap<String, TimestampedValue<String>> data = new HashMap<>();

        // É líder
        if (selfAddress == leaderAddress && selfPort == leaderPort) {
            while (!serverSocket.isClosed()) {
                final Socket clientSocket = serverSocket.accept();
                new Thread(() -> {
                    try (
                            final ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
                            final ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                    ) {
                        while (!clientSocket.isClosed()) {
                            Mensagem msg = (Mensagem) in.readObject();
                            switch (msg.code) {
                                case PUT:
                                    synchronized (data) {
                                        final TimestampedValue<String> timestampedValue = new TimestampedValue<>(msg.value);
                                        data.put(msg.key, timestampedValue);
                                        out.writeObject(new Mensagem(
                                                Mensagem.Code.PUT_OK,
                                                msg.key,
                                                null,
                                                timestampedValue.timestamp
                                        ));
                                    }
                                    break;
                                case GET:
                                    synchronized (data) {
                                        final TimestampedValue<String> timestampedValue = data.getOrDefault(msg.key, null);
                                        final String value = (timestampedValue != null && timestampedValue.timestamp >= msg.timestamp)?
                                                timestampedValue.value : null;
                                        final long timestamp = (value != null)?
                                                timestampedValue.timestamp : msg.timestamp;
                                        out.writeObject(new Mensagem(
                                                Mensagem.Code.GET,
                                                msg.key,
                                                value,
                                                timestamp
                                        ));
                                    }
                                default:
                            }
                        }
                    } catch (IOException | ClassNotFoundException exception) {
                        throw new RuntimeException(exception);
                    }
                }).start();
            }
            return;
        }

        // Não é líder
        while (!serverSocket.isClosed()) {
            final Socket clientSocket = serverSocket.accept();
            new Thread(() -> {
                // TODO: Non-leader implementation
            }).start();
        }
    }
}
