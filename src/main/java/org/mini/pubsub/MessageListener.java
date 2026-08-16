package org.mini.pubsub;

@FunctionalInterface
public interface MessageListener {
    void onMessage(String topic, byte[] payload);
}
