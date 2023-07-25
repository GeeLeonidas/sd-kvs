package br.dev.gee;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("BusyWait")
public class Servidor {
    public static class TimestampedValue<T> {
        public final long timestamp;
        public final T value;

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
            if (input.isEmpty())
                return DEFAULT_PORT;
            // Portas válidas (Fonte: https://ihateregex.io/expr/port)
            if (input.matches("^((6553[0-5])|(655[0-2][0-9])|(65[0-4][0-9]{2})|(6[0-4][0-9]{3})|([1-5][0-9]{4})|([0-5]{0,5})|([0-9]{1,4}))$"))
                return Integer.parseInt(input);
        }
    }

    public static void main(String[] args) throws IOException {
        final Scanner scanner = new Scanner(System.in);
        // Entrada dos dados do servidor atual
        final InetAddress selfAddress = readAddress(scanner, "Host");
        final int selfPort = readPort(scanner, "Host");
        // Entrada dos dados do servidor líder
        final InetAddress leaderAddress = readAddress(scanner, "Líder");
        final int leaderPort = readPort(scanner, "Líder");
        scanner.close();

        final ServerSocket serverSocket = new ServerSocket(selfPort, 50, selfAddress);
        final HashMap<String, TimestampedValue<String>> data = new HashMap<>();

        // É líder
        if (selfAddress.equals(leaderAddress) && selfPort == leaderPort) {
            final HashSet<NetworkInfo> nonLeaders = new HashSet<>();
            final HashMap<NetworkInfo, HashSet<String>> nonLeadersOutdatedKeys = new HashMap<>();
            final HashMap<NetworkInfo, HashSet<String>> nonLeadersToSendReplication = new HashMap<>();

            AtomicLong currentTimestamp = new AtomicLong(0L);

            while (!serverSocket.isClosed()) {
                final Socket clientSocket = serverSocket.accept();
                // Uso do 'Socket' feito por outra Thread, principal apenas escuta 'serverSocket'
                new Thread(() -> {
                    try (
                            final ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                            final ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream())
                    ) {
                        final NetworkInfo info = new NetworkInfo(clientSocket.getInetAddress(), clientSocket.getPort());
                        final Mensagem firstMsg = (Mensagem) in.readObject();
                        if (firstMsg.code == Mensagem.Code.SERVER_HERE) {
                            // Conexão trata-se de outro servidor
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
                            // Conexão não se trata de um cliente
                            clientSocket.close();
                            return;
                        }

                        HashMap<String, Runnable> putOkEvents = new HashMap<>();
                        new Thread(() -> { // Execução em segundo plano
                            try {
                                while (!clientSocket.isClosed()) {
                                    // Envia 'REPLICATION' caso o servidor conectado esteja marcado como interessado
                                    synchronized (nonLeadersToSendReplication) {
                                        final HashSet<String> set = nonLeadersToSendReplication.get(info);
                                        if (firstMsg.code == Mensagem.Code.SERVER_HERE && !set.isEmpty()) {
                                            for (String key : set) {
                                                synchronized (data) {
                                                    TimestampedValue<String> entry = data.get(key);
                                                    synchronized (out) {
                                                        out.writeObject(new Mensagem(
                                                                Mensagem.Code.REPLICATION,
                                                                key,
                                                                entry.value,
                                                                entry.timestamp
                                                        ));
                                                    }
                                                }
                                            }
                                            set.clear();
                                            nonLeadersToSendReplication.put(info, set);
                                        }
                                    }

                                    synchronized (putOkEvents) {
                                        for (String key : putOkEvents.keySet()) {
                                            // Verifica se todos os outros servidores estão atualizados
                                            boolean shouldExecute = true;
                                            synchronized (nonLeadersOutdatedKeys) {
                                                for (NetworkInfo nonLeaderInfo : nonLeadersOutdatedKeys.keySet()) {
                                                    HashSet<String> set = nonLeadersOutdatedKeys.get(nonLeaderInfo);
                                                    if (!set.isEmpty() && set.contains(key)) {
                                                        // Um servidor ainda está desatualizado
                                                        shouldExecute = false;
                                                        break;
                                                    }
                                                }
                                            }
                                            if (shouldExecute) {
                                                // Executa o envio de 'PUT_OK' e o consome
                                                putOkEvents.get(key).run();
                                                putOkEvents.remove(key);
                                            }
                                        }
                                    }

                                    try {
                                        Thread.sleep(0, 5000); // Delay de 5 microsegundos para evitar consumo de CPU
                                    } catch (InterruptedException ignored) {}
                                }
                            } catch (IOException exception) {
                                if (!(exception instanceof EOFException))
                                    throw new RuntimeException(exception);
                            }
                        }).start();
                        while (!clientSocket.isClosed()) {
                            final Mensagem msg = (Mensagem) in.readObject();
                            switch (msg.code) {
                                case PUT:
                                    // Atribua valor à chave 'msg.key' na tabela hash 'data'
                                    synchronized (data) {
                                        final TimestampedValue<String> timestampedValue = new TimestampedValue<>(
                                                msg.value,
                                                currentTimestamp.addAndGet(1)
                                        );
                                        data.put(msg.key, timestampedValue);
                                    }
                                    // Agende uma execução do envio de 'PUT_OK' para ser consumida no futuro
                                    synchronized (putOkEvents) {
                                        putOkEvents.put(msg.key, () -> {
                                            synchronized (data) {
                                                try {
                                                    final long timestamp = data.get(msg.key).timestamp;
                                                    synchronized (out) {
                                                        out.writeObject(new Mensagem(
                                                                Mensagem.Code.PUT_OK,
                                                                msg.key,
                                                                null,
                                                                timestamp
                                                        ));
                                                    }
                                                    System.out.printf("Enviando PUT_OK ao Cliente %s da key:%s ts:%d\n", info, msg.key, timestamp);
                                                } catch (IOException exception) {
                                                    if (!(exception instanceof EOFException))
                                                        throw new RuntimeException(exception);
                                                }
                                            }
                                        });
                                    }
                                    // Marca todos os outros servidores como desatualizados e interessados em receber 'REPLICATION'
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
                                    System.out.printf("Cliente %s PUT key:%s value:%s\n", info, msg.key, msg.value);
                                    break;
                                case GET:
                                    synchronized (data) {
                                        if (!data.containsKey(msg.key)) {
                                            // Devolve 'null' por não possuir a chave
                                            synchronized (out) {
                                                out.writeObject(new Mensagem(
                                                        Mensagem.Code.GET,
                                                        msg.key,
                                                        null,
                                                        msg.timestamp
                                                ));
                                            }
                                            System.out.printf(
                                                    "Cliente %s GET key:%s ts:%d. Meu ts é 0, portanto devolvendo null\n",
                                                    info,
                                                    msg.key,
                                                    msg.timestamp
                                            );
                                            break;
                                        }
                                        TimestampedValue<String> entry = data.get(msg.key);
                                        // Devolve 'entry.value' caso o cliente informe um timestamp menor ou igual ao disponível
                                        synchronized (out) {
                                            out.writeObject(new Mensagem(
                                                    Mensagem.Code.GET,
                                                    msg.key,
                                                    entry.value,
                                                    entry.timestamp
                                            ));
                                        }
                                        System.out.printf(
                                                "Cliente %s GET key:%s ts:%d. Meu ts é %d, portanto devolvendo %s\n",
                                                info,
                                                msg.key,
                                                msg.timestamp,
                                                entry.timestamp,
                                                entry.value
                                        );
                                    }
                                    break;
                                case REPLICATION_OK:
                                    if (firstMsg.code == Mensagem.Code.CLIENT_HERE)
                                        break;
                                    // Remove o servidor do conjunto de desatualizados
                                    synchronized (nonLeadersOutdatedKeys) {
                                        final HashSet<String> set = nonLeadersOutdatedKeys.get(info);
                                        set.remove(msg.key);
                                        nonLeadersOutdatedKeys.put(info, set);
                                    }
                                default:
                            }
                        }
                    } catch (IOException | ClassNotFoundException exception) {
                        if (!(exception instanceof EOFException))
                            throw new RuntimeException(exception);
                    }
                }).start();
            }
            return;
        }

        // Não é líder
        final Socket leaderSocket = new Socket(leaderAddress, leaderPort);
        try (
                final ObjectOutputStream leaderOut = new ObjectOutputStream(leaderSocket.getOutputStream());
                final ObjectInputStream leaderIn = new ObjectInputStream(leaderSocket.getInputStream())
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
                                // Recebe a requisição REPLICATION e atualiza a tabela hash
                                synchronized (data) {
                                    data.put(msg.key, new TimestampedValue<>(msg.value, msg.timestamp));
                                }
                                // Envia REPLICATION_OK como resposta
                                synchronized (leaderOut) {
                                    leaderOut.writeObject(new Mensagem(
                                            Mensagem.Code.REPLICATION_OK,
                                            msg.key,
                                            null,
                                            msg.timestamp
                                    ));
                                }
                                System.out.printf("REPLICATION key:%s value:%s ts:%d\n", msg.key, msg.value, msg.timestamp);
                                break;
                            case PUT_OK:
                                // Agenda uma mensagem PUT_OK para ser enviada aos clientes interessados
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
                    if (!(e instanceof EOFException))
                        throw new RuntimeException(e);
                }
            }).start();
            while (!serverSocket.isClosed()) {
                final Socket clientSocket = serverSocket.accept();
                // Uso do 'Socket' feito por outra Thread, principal apenas escuta 'serverSocket'
                new Thread(() -> {
                    try (
                            final ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                            final ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream())
                    ) {
                        final NetworkInfo info = new NetworkInfo(clientSocket.getInetAddress(), clientSocket.getPort());
                        final Mensagem firstMsg = (Mensagem) in.readObject();
                        if (firstMsg.code != Mensagem.Code.CLIENT_HERE) {
                            clientSocket.close();
                            return;
                        }

                        new Thread(() -> { // Execução em segundo plano
                            try {
                                while (!clientSocket.isClosed()) {
                                    synchronized (toBeSentPutOks) {
                                        // Para cada mensagem, cheque se este cliente está interessado
                                        for (Mensagem msg : toBeSentPutOks.keySet()) {
                                            HashSet<NetworkInfo> set = toBeSentPutOks.getOrDefault(msg, new HashSet<>());
                                            if (set.contains(info)) {
                                                // Envio da mensagem PUT_OK ao cliente
                                                synchronized (out) {
                                                    out.writeObject(msg);
                                                }
                                                set.remove(info);
                                            }
                                            if (set.isEmpty())
                                                toBeSentPutOks.remove(msg);
                                            else
                                                toBeSentPutOks.put(msg, set);
                                        }
                                    }
                                    try {
                                        Thread.sleep(0, 5000); // Delay de 5 microsegundos para evitar consumo de CPU
                                    } catch (InterruptedException ignored) {}
                                }
                            } catch (IOException exception) {
                                if (!(exception instanceof EOFException))
                                    throw new RuntimeException(exception);
                            }
                        }).start();
                        while (!clientSocket.isClosed()) {
                            final Mensagem msg = (Mensagem) in.readObject();
                            switch (msg.code) {
                                case PUT:
                                    // Encaminha requisição PUT ao líder
                                    synchronized (leaderOut) {
                                        leaderOut.writeObject(msg);
                                    }
                                    // Sinaliza que o cliente está interessado na resposta PUT_OK atrelada à 'msg.key'
                                    synchronized (putOkInterestedClients) {
                                        HashSet<NetworkInfo> set = putOkInterestedClients.getOrDefault(msg.key, new HashSet<>());
                                        set.add(info);
                                        putOkInterestedClients.put(msg.key, set);
                                    }
                                    System.out.printf("Encaminhando PUT key:%s value:%s\n", msg.key, msg.value);
                                    break;
                                case GET:
                                    synchronized (data) {
                                        if (!data.containsKey(msg.key)) {
                                            // O servidor não possui a chave requisitada pelo cliente
                                            if (msg.timestamp > 0) {
                                                // Devolve 'TRY_OTHER_SERVER_OR_LATER' caso o cliente informe um timestamp válido (> 0)
                                                synchronized (out) {
                                                    out.writeObject(new Mensagem(
                                                            Mensagem.Code.TRY_OTHER_SERVER_OR_LATER,
                                                            msg.key,
                                                            null,
                                                            msg.timestamp
                                                    ));
                                                }
                                                System.out.printf(
                                                        "Cliente %s GET key:%s ts:%d. Meu ts é 0, portanto devolvendo TRY_OTHER_SERVER_OR_LATER\n",
                                                        info,
                                                        msg.key,
                                                        msg.timestamp
                                                );
                                                break;
                                            }
                                            // Devolve 'null' caso seja um timestamp placeholder (<= 0)
                                            synchronized (out) {
                                                out.writeObject(new Mensagem(
                                                        Mensagem.Code.GET,
                                                        msg.key,
                                                        null,
                                                        msg.timestamp
                                                ));
                                            }
                                            System.out.printf(
                                                    "Cliente %s GET key:%s ts:%d. Meu ts é 0, portanto devolvendo null\n",
                                                    info,
                                                    msg.key,
                                                    msg.timestamp
                                            );
                                            break;
                                        }
                                        TimestampedValue<String> entry = data.get(msg.key);
                                        if (entry.timestamp < msg.timestamp) {
                                            // Devolve 'TRY_OTHER_SERVER_OR_LATER' caso o cliente informe um timestamp menor que o disponível
                                            synchronized (out) {
                                                out.writeObject(new Mensagem(
                                                        Mensagem.Code.TRY_OTHER_SERVER_OR_LATER,
                                                        msg.key,
                                                        null,
                                                        msg.timestamp
                                                ));
                                            }
                                            System.out.printf(
                                                    "Cliente %s GET key:%s ts:%d. Meu ts é %d, portanto devolvendo TRY_OTHER_SERVER_OR_LATER\n",
                                                    info,
                                                    msg.key,
                                                    msg.timestamp,
                                                    entry.timestamp
                                            );
                                            break;
                                        }
                                        // Devolve 'entry.value' caso o cliente informe um timestamp menor ou igual ao disponível
                                        synchronized (out) {
                                            out.writeObject(new Mensagem(
                                                    Mensagem.Code.GET,
                                                    msg.key,
                                                    entry.value,
                                                    entry.timestamp
                                            ));
                                        }
                                        System.out.printf(
                                                "Cliente %s GET key:%s ts:%d. Meu ts é %d, portanto devolvendo %s\n",
                                                info,
                                                msg.key,
                                                msg.timestamp,
                                                entry.timestamp,
                                                entry.value
                                        );
                                    }
                                default:
                            }
                        }
                    } catch (IOException | ClassNotFoundException exception) {
                        if (!(exception instanceof EOFException))
                            throw new RuntimeException(exception);
                    }
                }).start();
            }
        }
    }
}
