package nettytest;

import io.Listener;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;

import topology.Assignment;
import topology.ClusterNode;

public class NettyListener extends SimpleChannelHandler implements Listener {
    private BlockingQueue<byte[]> handoffQueue = new SynchronousQueue<byte[]>();
    private ClusterNode node;
    
    public NettyListener(Assignment assignment) {
        // wait for an assignment
        node = assignment.assignPartition();
        
        ChannelFactory factory =
            new NioServerSocketChannelFactory(
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool());

        ServerBootstrap bootstrap = new ServerBootstrap(factory);

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {
                ChannelPipeline p = Channels.pipeline();
                p.addLast("1", new LengthFieldBasedFrameDecoder(999999, 0, 4, 0, 4));
                p.addLast("2", new ChannelHandler(handoffQueue));
                
                return p;
            }
        });

        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
        
        bootstrap.bind(new InetSocketAddress(node.getPort()));
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
    
    public class ChannelHandler extends SimpleChannelUpstreamHandler {
        private BlockingQueue<byte[]> handoffQueue;
        
        public ChannelHandler(BlockingQueue<byte[]> handOffQueue) {
            this.handoffQueue = handOffQueue;
        }
        
        public void messageReceived(ChannelHandlerContext ctx,
                MessageEvent e) {
            ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
            try {
                handoffQueue.put(buffer.array()); // this holds up the Netty upstream I/O thread if
                                                  // there's no receiver at the other end of the handoff queue
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }  
    }
}
