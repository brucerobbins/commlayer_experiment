package io;

public interface Listener {
    byte[] recv();
    public int getPartitionId();
}
