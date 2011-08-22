package io;

public interface Emitter {
    void send(int partitionId, byte[] message);
    int getPartitionCount();
}
