package generictest;

import org.apache.log4j.Logger;

import io.Emitter;
import io.Listener;
import io.QueueingListener;

public class Node {
    private long totalMessageCount = 0;
    private long badMessageCount = 0;
    private long consumeMessageCount = 0;
    private Emitter emitter;
    private Listener listener;
    private int partitionCount;
    private Logger logger = Logger.getLogger(this.getClass());

    public Node(Emitter emitter, Listener listener) {
        this.emitter = emitter;
        this.listener = listener;
        this.partitionCount = emitter.getPartitionCount();
        final int partitionId = listener.getPartitionId();

        Runnable displayer = new Runnable() {
            public void run() {
                while (true) {
                    logger.info(String
                            .format("%d: total: %d, bad: %d, consumed %d, dropped %d",
                                    partitionId,
                                    totalMessageCount,
                                    badMessageCount,
                                    consumeMessageCount,
                                    Node.this.listener instanceof QueueingListener ? ((QueueingListener) Node.this.listener)
                                            .getDropCount() : -1));
                    try {
                        Thread.sleep(15000);
                    } catch (InterruptedException e) {
                        // nothing to do here
                    }
                }
            }

        };

        Thread t = new Thread(displayer);
        t.start();
    }

    public void run() {
        while (true) {
            String message = new String(listener.recv());
            // message structure is
            // <key> (consume|forward) <random-junk>
            String[] tokens = message.split(" ");

            totalMessageCount++;
            if (tokens.length != 3) {
                badMessageCount++;
                continue;
            }

            if (tokens[1].equals("consume")) {
                consumeMessageCount++;
            } else {
                // forward this message to someplace else
                // the first token is the key
                String newMessage = tokens[0] + " consume " + tokens[2];
                // get the hashkey for this message
                int targetPartitionId = (int) (Math.abs(RSHash(tokens[0])) % partitionCount);
                emitter.send(targetPartitionId, newMessage.getBytes());
            }
        }
    }

    /*
     * *************************************************************************
     * Hash function copied from * General Purpose Hash Function Algorithms
     * Library * * Author: Arash Partow - 2002 * URL: http://www.partow.net *
     * URL: http://www.partow.net/programming/hashfunctions/index.html * *
     * Copyright notice: * Free use of the General Purpose Hash Function
     * Algorithms Library is * permitted under the guidelines and in accordance
     * with the most current * version of the Common Public License. *
     * http://www.opensource.org/licenses/cpl1.0.php * *
     * *************************************************************************
     */
    public long RSHash(String str) {
        int b = 378551;
        int a = 63689;
        long hash = 0;

        for (int i = 0; i < str.length(); i++) {
            hash = hash * a + str.charAt(i);
            a = a * b;
        }

        return hash;
    }
}
