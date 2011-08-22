package generictest;

import java.util.Random;

import org.apache.log4j.Logger;

import io.Emitter;
import io.QueueingEmitter;
import topology.Cluster;
import topology.Config;

import pacer.Pacer;

public class Sender {
    private int expectedMessageCount = -1;
    private int expectedRate = -1;
    private int emitCount;
    private int displayRateInterval = 15;

    private Random randomizer = new Random(System.currentTimeMillis());

    private int partitionCount;
    private Emitter emitter;
    private Logger logger = Logger.getLogger(this.getClass());

    private String[] randomStrings;

    public Sender(Emitter emitter, int expectedMessageCount, int expectedRate,
            String[] randomStrings) {
        this.emitter = emitter;
        this.expectedMessageCount = expectedMessageCount;
        this.expectedRate = expectedRate;
        this.partitionCount = emitter.getPartitionCount();
        this.randomStrings = randomStrings;
    }

    public void run() {
        long intervalStart = 0;
        int emitCountStart = 0;
        int[] counts = new int[partitionCount];
        Pacer pacer = new Pacer(expectedRate);
        for (int key = 0; key < expectedMessageCount; key++) {
            pacer.startCycle();

            StringBuffer sb = new StringBuffer();
            sb.append(key);
            sb.append(" forward ");
            sb.append(randomStrings[randomizer.nextInt(randomStrings.length)]);

            String message = sb.toString();

            // choose a partition to emit on

            int partitionId = randomizer.nextInt(partitionCount);
            emitter.send(partitionId, message.getBytes());
            counts[partitionId]++;
            emitCount++;

            // if it's time, display the actual emit rate
            if (intervalStart == 0) {
                intervalStart = System.currentTimeMillis();
            } else {
                long interval = System.currentTimeMillis() - intervalStart;
                if (interval >= (displayRateInterval * 1000)) {
                    double rate = (emitCount - emitCountStart)
                            / (interval / 1000.0);
                    logger.info(String.format("Rate is %f, drop count is %d", rate,
                            emitter instanceof QueueingEmitter ? ((QueueingEmitter)emitter).getDropCount() : -1));
                    intervalStart = System.currentTimeMillis();
                    emitCountStart = emitCount;
                }
            }
            
            pacer.endCycle();
            pacer.maintainPace();
        }
        logger.info(String.format("Emitted %d events", emitCount));
        for (int i = 0; i < counts.length; i++) {
            logger.info(String.format("%d: %d", i, counts[i]));
        }
    }
}