package nettytest;

import io.Emitter;
import topology.ClusterNode;
import topology.TopologyChangeListener;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;

import topology.Topology;

public class NettyEmitter extends SimpleChannelHandler implements Emitter, ChannelFutureListener, TopologyChangeListener {
    private Topology topology;
    private Channel[] channels;
    private Logger logger = Logger.getLogger(this.getClass());
    
    public NettyEmitter(Topology topology) {
        this.topology = topology;
        
        channels = new Channel[topology.getTopology().getNodes().size()];
        ChannelFactory factory =
            new NioClientSocketChannelFactory(
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool());

        final ClientBootstrap bootstrap = new ClientBootstrap(factory);

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {
                ChannelPipeline p = Channels.pipeline();
                p.addLast("1", new LengthFieldPrepender(4));
                p.addLast("2", new TestHandler());
                return p;
            }
        });
        
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);
        
        Runnable r = new Runnable() {
            public void run() {
                for (ClusterNode clusterNode : NettyEmitter.this.topology.getTopology().getNodes()) {
                    int partition = clusterNode.getPartition();
                    logger.info(String.format("Connecting to %s:%d", clusterNode.getMachineName(), clusterNode.getPort()));
                    while (true) {
                        ChannelFuture f = bootstrap.connect(new InetSocketAddress(clusterNode.getMachineName(), clusterNode.getPort()));
                        f.awaitUninterruptibly();
                        if (f.isSuccess()) {
                            channels[partition] = f.getChannel();
                            break;
                        }
                        try {
                            Thread.sleep(1000);
                        }
                        catch (InterruptedException ie) {}
                    }
                }
                logger.info("Sender is connected to all " + channels.length + " receivers");
            }
        };
        (new Thread(r)).start();
    }
    
    private Object sendLock = new Object();
    
    public void send(int partitionId, byte[] message) {
        Channel channel = channels[partitionId];
        if (channel != null) {
            ChannelBuffer buffer = ChannelBuffers.buffer(message.length);
            
            // check if Netty's send queue has gotten quite large
            if (!channel.isWritable()) {
                synchronized(sendLock) {
                    // check again now that we have the lock
                    while (!channel.isWritable()) {
                        try {
                            sendLock.wait(); // wait until the channel's queue has gone down
                        } catch (InterruptedException ie) {
                            return; // somebody wants us to stop running
                        }
                    }
                    //logger.info("Woke up from send block!");
                }
            }
            // between the above isWritable check and the below writeBytes, the isWritable
            // may become false again. That's OK, we're just trying to avoid a very large
            // above check to avoid creating a very large send queue inside Netty.
            buffer.writeBytes(message);
            ChannelFuture f = channel.write(buffer);
            f.addListener(this);
        }
    }
    
    public void operationComplete(ChannelFuture f) {
        // when we get here, the I/O operation associated with f is complete
        if (f.isCancelled()) {
            logger.error("Send I/O was cancelled!! " + f.getChannel().getRemoteAddress());
        }
        else if (!f.isSuccess()) {
            logger.error("Exception on I/O operation", f.getCause());
            // find the partition associated with this broken channel
            Channel channel = f.getChannel();
            int partition = -1;
            for (int i = 0; i < channels.length; i++) {
                if (channels[i] == channel) {
                    partition = i;
                    break;
                }
            }
            logger.error(String.format("I/O on partition %d failed!", partition));
        }
    }
    
    public void onChange() {
        // do nothing for now, don't expect the topology to change.
    }
    
    public int getPartitionCount() {
        return topology.getTopology().getNodes().size();
    }
    
    class TestHandler extends SimpleChannelHandler {
        public void channelInterestChanged(ChannelHandlerContext ctx,
                ChannelStateEvent e) {
            //logger.info(String.format("%08x %08x %08x", e.getValue(), e.getChannel().getInterestOps(), Channel.OP_WRITE));
            synchronized (sendLock) {
                if (e.getChannel().isWritable()) {
                    sendLock.notify();
                }
            }
            ctx.sendUpstream(e);
            
        }
        
        public void exceptionCaught(ChannelHandlerContext context, ExceptionEvent event) {
            Throwable cause = event.getCause();
            if (cause instanceof java.net.ConnectException) {
                return;
            }
            cause.printStackTrace();
        }
    }
}    

