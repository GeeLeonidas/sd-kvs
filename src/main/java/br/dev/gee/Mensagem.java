package br.dev.gee;

import java.io.Serializable;

public class Mensagem implements Serializable {
    public enum Code {
        SERVER_HERE, CLIENT_HERE,
        PUT, PUT_OK,
        REPLICATION, REPLICATION_OK,
        GET, TRY_OTHER_SERVER_OR_LATER
    }

    public final Code code;
    public final String key, value;
    public final long timestamp;

    public Mensagem(Code code, String key, String value, long timestamp) {
        this.code = code;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }
}
