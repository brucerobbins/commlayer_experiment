package udptest;

import io.Listener;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import topology.Assignment;
import topology.ClusterNode;

public class UDPListener implements Runnable, Listener {
    private DatagramSocket socket;
    private DatagramPacket datagram;
    private byte[] bs;
    static int BUFFER_LENGTH = 65507;
    private BlockingQueue<byte[]> handoffQueue = new SynchronousQueue<byte[]>();
    private ClusterNode node;

    public UDPListener(Assignment assignment, int UDPBufferSize) {
        // wait for an assignment
        node = assignment.assignPartition();
        
        try {
            socket = new DatagramSocket(node.getPort());
            if (UDPBufferSize > 0) {
                socket.setReceiveBufferSize(UDPBufferSize);
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        bs = new byte[BUFFER_LENGTH];
        datagram = new DatagramPacket(bs, bs.length);
        (new Thread(this)).start();
    }

    public void run() {
        try {
            while (!Thread.interrupted()) {
                socket.receive(datagram);
                byte[] data = new byte[datagram.getLength()];
                System.arraycopy(datagram.getData(), datagram.getOffset(), data, 0,
                        data.length);
                datagram.setLength(BUFFER_LENGTH);
                try {
                    handoffQueue.put(data);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public byte[] recv() {
        try {
            return handoffQueue.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
    public int getPartitionId() {
        return node.getPartition();
    }
}
