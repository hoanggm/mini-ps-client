package org.mini.pubsub.client;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClientConfig {
    private final String host;
    private final int port;
    private final int connectTimeoutMs;
    private final boolean autoReconnect;
    private final List<InetSocketAddress> serverAddresses;
    private final boolean isClusterMode;

    public ClientConfig(String host, int port) {
        this(host, port, 5000, true);
    }

    public ClientConfig(String host, int port, int connectTimeoutMs, boolean autoReconnect) {
        this.host = host;
        this.port = port;
        this.connectTimeoutMs = connectTimeoutMs;
        this.autoReconnect = autoReconnect;
        this.serverAddresses = null;
        this.isClusterMode = false;
    }

    public ClientConfig(String clusterNodes, int connectTimeoutMs, boolean autoReconnect) {
        this.host = null;
        this.port = 0;
        this.connectTimeoutMs = connectTimeoutMs;
        this.autoReconnect = autoReconnect;
        this.isClusterMode = true;
        this.serverAddresses = new ArrayList<>();
        if (clusterNodes != null && !clusterNodes.isBlank()) {
            String[] nodes = clusterNodes.split(",");
            for (String node : nodes) {
                String[] parts = node.trim().split(":");
                if (parts.length == 2) {
                    String iHost = parts[0].trim();
                    int iPort = Integer.parseInt(parts[1].trim());
                    this.serverAddresses.add(new InetSocketAddress(iHost, iPort));
                }
            }
        }

        if (this.serverAddresses.isEmpty()) {
            throw new IllegalArgumentException("Invalid clusterNodes format");
        }
        // shuffle for load balance
        Collections.shuffle(this.serverAddresses);
    }

    public List<InetSocketAddress> getServerAddresses() {
        return serverAddresses;
    }

    public Boolean getIsClusterMode() {
        return isClusterMode;
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
