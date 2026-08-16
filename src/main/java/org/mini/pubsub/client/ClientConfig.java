package org.mini.pubsub.client;

public class ClientConfig {
    private final String host;
    private final int port;
    private final int connectTimeoutMs;
    private final boolean autoReconnect;

    public ClientConfig(String host, int port) {
        this(host, port, 5000, true);
    }

    public ClientConfig(String host, int port, int connectTimeoutMs, boolean autoReconnect) {
        this.host = host;
        this.port = port;
        this.connectTimeoutMs = connectTimeoutMs;
        this.autoReconnect = autoReconnect;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public boolean isAutoReconnect() {
        return autoReconnect;
    }
}
