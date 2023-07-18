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
import java.util.HashSet;
import java.util.Scanner;

public class Servidor {
    public static class TimestampedValue<T> {
        public final long timestamp;
        public final T value;

        public TimestampedValue(T value) {
            this.timestamp = Instant.now().toEpochMilli();
            this.value = value;
        }

        public TimestampedValue(T value, long timestamp) {
            this.timestamp = timestamp;
            this.value = value;
        }
    }

    public static class NetworkInfo {
        public final InetAddress address;
        public final int port;

        public NetworkInfo(InetAddress address, int port) {
            this.address = address;
            this.port = port;
        }

        @Override
        public String toString() {
            return String.format("%s:%d", this.address.getHostAddress(), this.port);
        }

        @Override
        public int hashCode() {
            return this.toString().hashCode();
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
        if (selfAddress.equals(leaderAddress) && selfPort == leaderPort) {
            final HashSet<NetworkInfo> nonLeaders = new HashSet<>();
            final HashMap<NetworkInfo, HashSet<String>> nonLeadersOutdatedKeys = new HashMap<>();
            final HashMap<NetworkInfo, HashSet<String>> nonLeadersToSendReplication = new HashMap<>();

            while (!serverSocket.isClosed()) {
                final Socket clientSocket = serverSocket.accept();
                new Thread(() -> {
                    try (
                            final ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
                            final ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream())
                    ) {
                        final NetworkInfo info = new NetworkInfo(clientSocket.getInetAddress(), clientSocket.getPort());
                        final Mensagem firstMsg = (Mensagem) in.readObject();
                        if (firstMsg.code == Mensagem.Code.SERVER_HERE) {
                            synchronized (nonLeaders) {
                                nonLeaders.add(info);
                            }
                            synchronized (nonLeadersOutdatedKeys) {
                                nonLeadersOutdatedKeys.put(info, new HashSet<>());
                            }
                            synchronized (nonLeadersToSendReplication) {
                                nonLeadersToSendReplication.put(info, new HashSet<>());
                            }
                        } else if (firstMsg.code != Mensagem.Code.CLIENT_HERE) {
                            clientSocket.close();
                            return;
                        }

                        HashMap<String, Runnable> putOkEvents = new HashMap<>();
                        while (!clientSocket.isClosed()) {
                            if (in.available() == 0) { // Caso não tenha recebido mensagem
                                synchronized (nonLeadersToSendReplication) {
                                    final HashSet<String> set = nonLeadersToSendReplication.get(info);
                                    if (firstMsg.code == Mensagem.Code.SERVER_HERE && !set.isEmpty()) {
                                        for (String key : set) {
                                            synchronized (data) {
                                                TimestampedValue<String> entry = data.get(key);
                                                out.writeObject(new Mensagem(
                                                        Mensagem.Code.REPLICATION,
                                                        key,
                                                        entry.value,
                                                        entry.timestamp
                                                ));
                                            }
                                        }
                                        set.clear();
                                        nonLeadersToSendReplication.put(info, set);
                                    }
                                }

                                for (String key : putOkEvents.keySet()) {
                                    boolean shouldExecute = true;
                                    synchronized (nonLeadersOutdatedKeys) {
                                        for (NetworkInfo nonLeaderInfo : nonLeadersOutdatedKeys.keySet()) {
                                            HashSet<String> set = nonLeadersOutdatedKeys.get(nonLeaderInfo);
                                            if (!set.isEmpty() && set.contains(key)) {
                                                shouldExecute = false;
                                                break;
                                            }
                                        }
                                    }
                                    if (shouldExecute) {
                                        putOkEvents.get(key).run();
                                        putOkEvents.remove(key);
                                    }
                                }

                                try {
                                    in.wait(0, 5000); // Delay de cinco microsegundos para evitar consumo de CPU
                                } catch (InterruptedException ignored) {}
                                continue;
                            }

                            final Mensagem msg = (Mensagem) in.readObject();
                            switch (msg.code) {
                                case PUT:
                                    synchronized (nonLeaders) {
                                        for (NetworkInfo nonLeader : nonLeaders) {
                                            synchronized (nonLeadersOutdatedKeys) {
                                                HashSet<String> set = nonLeadersOutdatedKeys.get(nonLeader);
                                                set.add(msg.key);
                                                nonLeadersOutdatedKeys.put(nonLeader, set);
                                            }
                                            synchronized (nonLeadersToSendReplication) {
                                                final HashSet<String> set = nonLeadersToSendReplication.get(nonLeader);
                                                set.add(msg.key);
                                                nonLeadersToSendReplication.put(nonLeader, set);
                                            }
                                        }
                                    }
                                    synchronized (data) {
                                        final TimestampedValue<String> timestampedValue = new TimestampedValue<>(msg.value);
                                        data.put(msg.key, timestampedValue);
                                    }
                                    putOkEvents.put(msg.key, () -> {
                                        synchronized (data) {
                                            try {
                                                out.writeObject(new Mensagem(
                                                        Mensagem.Code.PUT_OK,
                                                        msg.key,
                                                        null,
                                                        data.get(msg.key).timestamp
                                                ));
                                            } catch (IOException exception) {
                                                throw new RuntimeException(exception);
                                            }
                                        }
                                    });
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
                                    break;
                                case REPLICATION_OK:
                                    if (firstMsg.code == Mensagem.Code.CLIENT_HERE)
                                        break;
                                    synchronized (nonLeadersOutdatedKeys) {
                                        final HashSet<String> set = nonLeadersOutdatedKeys.get(info);
                                        set.remove(msg.key);
                                        nonLeadersOutdatedKeys.put(info, set);
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
        final Socket leaderSocket = new Socket(leaderAddress, leaderPort);
        try (
                final ObjectInputStream leaderIn = new ObjectInputStream(leaderSocket.getInputStream());
                final ObjectOutputStream leaderOut = new ObjectOutputStream(leaderSocket.getOutputStream())
        ) {
            final HashMap<String, HashSet<NetworkInfo>> putOkInterestedClients = new HashMap<>();
            final HashMap<Mensagem, HashSet<NetworkInfo>> toBeSentPutOks = new HashMap<>();

            leaderOut.writeObject(new Mensagem(
                    Mensagem.Code.SERVER_HERE,
                    null,
                    null,
                    -1
            ));
            new Thread(() -> { // Recebimento das mensagens do líder
                try {
                    while (!leaderSocket.isClosed()) {
                        Mensagem msg = (Mensagem) leaderIn.readObject();
                        switch (msg.code) {
                            case REPLICATION:
                                synchronized (data) {
                                    data.put(msg.key, new TimestampedValue<>(msg.value, msg.timestamp));
                                }
                                synchronized (leaderOut) {
                                    leaderOut.writeObject(new Mensagem(
                                            Mensagem.Code.REPLICATION_OK,
                                            msg.key,
                                            null,
                                            msg.timestamp
                                    ));
                                }
                                break;
                            case PUT_OK:
                                synchronized (toBeSentPutOks) {
                                    synchronized (putOkInterestedClients) {
                                        toBeSentPutOks.put(new Mensagem(
                                                Mensagem.Code.PUT_OK,
                                                msg.key,
                                                null,
                                                msg.timestamp
                                        ), putOkInterestedClients.get(msg.key));
                                    }
                                }
                                break;
                            default:
                        }
                    }
                } catch (IOException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }).start();
            while (!serverSocket.isClosed()) {
                final Socket clientSocket = serverSocket.accept();
                new Thread(() -> {
                    try (
                            final ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
                            final ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream())
                    ) {
                        final NetworkInfo info = new NetworkInfo(clientSocket.getInetAddress(), clientSocket.getPort());
                        final Mensagem firstMsg = (Mensagem) in.readObject();
                        if (firstMsg.code != Mensagem.Code.CLIENT_HERE) {
                            clientSocket.close();
                            return;
                        }

                        while (!clientSocket.isClosed()) {
                            synchronized (toBeSentPutOks) {
                                for (Mensagem msg : toBeSentPutOks.keySet()) {
                                    HashSet<NetworkInfo> set = toBeSentPutOks.getOrDefault(msg, new HashSet<>());
                                    if (set.contains(info))
                                        out.writeObject(msg);
                                }
                            }

                            if (in.available() == 0) { // Caso não tenha recebido a mensagem de um cliente
                                try {
                                    in.wait(0, 5000); // Delay de cinco microsegundos para evitar consumo de CPU
                                } catch (InterruptedException ignored) {}
                                continue;
                            }

                            final Mensagem msg = (Mensagem) in.readObject();
                            switch (msg.code) {
                                case PUT:
                                    synchronized (leaderOut) {
                                        leaderOut.writeObject(msg);
                                    }
                                    synchronized (putOkInterestedClients) {
                                        HashSet<NetworkInfo> set = putOkInterestedClients.getOrDefault(msg.key, new HashSet<>());
                                        set.add(info);
                                        putOkInterestedClients.put(msg.key, set);
                                    }
                                    break;
                                case GET:
                                    synchronized (data) {
                                        if (!data.containsKey(msg.key)) {
                                            if (msg.timestamp >= 0) {
                                                out.writeObject(new Mensagem(
                                                        Mensagem.Code.TRY_OTHER_SERVER_OR_LATER,
                                                        msg.key,
                                                        null,
                                                        msg.timestamp
                                                ));
                                                break;
                                            }
                                            out.writeObject(new Mensagem(
                                                    Mensagem.Code.GET,
                                                    msg.key,
                                                    null,
                                                    msg.timestamp
                                            ));
                                            break;
                                        }
                                        TimestampedValue<String> entry = data.get(msg.key);
                                        if (entry.timestamp < msg.timestamp) {
                                            out.writeObject(new Mensagem(
                                                    Mensagem.Code.TRY_OTHER_SERVER_OR_LATER,
                                                    msg.key,
                                                    null,
                                                    msg.timestamp
                                            ));
                                            break;
                                        }
                                        out.writeObject(new Mensagem(
                                                Mensagem.Code.GET,
                                                msg.key,
                                                entry.value,
                                                entry.timestamp
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
        }
    }
}
