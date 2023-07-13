package br.dev.gee;

import java.io.Serializable;

public class Mensagem implements Serializable {
    public enum Code {
        PUT, PUT_OK,
        REPLICATION, REPLICATION_OK,
        GET, TRY_OTHER_SERVER_OR_LATER
    }

    final Code code;
    final String key, value;
    final Long timestamp;

    public Mensagem(Code code, String key, String value, long timestamp) {
        this.code = code;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }
}
