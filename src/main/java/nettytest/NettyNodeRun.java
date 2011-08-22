package nettytest;

import io.Emitter;
import io.Listener;
import io.QueueingEmitter;
import io.QueueingListener;

import java.io.File;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import topology.Assignment;
import topology.AssignmentFromFile;
import topology.Topology;
import topology.TopologyFromFile;

import nettytest.NettyListener;
import generictest.Node;

public class NettyNodeRun {
    public static Logger logger = Logger.getLogger(NettyNodeRun.class);
    
    public static void main(String[] args) {
        BasicConfigurator.configure();
        
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("queuesize").hasArg()
                .withDescription("Listener Queue Size").create("q"));

        CommandLineParser parser = new GnuParser();

        CommandLine commandLine = null;
        try {
            // parse the command line arguments
            commandLine = parser.parse(options, args);
        } catch (ParseException exp) {
            // oops, something went wrong
            logger.error("Parsing failed.  Reason: " + exp.getMessage());
            System.exit(1);
        }

        int queueSize = 87;
        if (commandLine.hasOption("q")) {
            try {
                queueSize = Integer.parseInt(commandLine.getOptionValue("q"));
            } catch (NumberFormatException e) {
                logger.error("Bad queue "
                        + commandLine.getOptionValue("q"));
                System.exit(1);
            }
        }

        List loArgs = commandLine.getArgList();

        if (loArgs.size() < 1) {
            logger.error("No cluster name specified");
            System.exit(1);
        }
        
        String clusterName = (String) loArgs.get(0);

        if (loArgs.size() < 2) {
            logger.error("No configuration file specified");
            System.exit(1);
        }
        
        String configFilename = (String)loArgs.get(1);
        if (!(new File(configFilename)).exists()) {
            logger.error(String.format("Specified configuration file %s does not exist", configFilename));
        }
        
        Assignment assignment = new AssignmentFromFile(clusterName, configFilename);
        Topology topology = new TopologyFromFile(clusterName, configFilename);
        
        logger.info("Creating listener");
        Listener lowLevelListener = new NettyListener(assignment);
        logger.info("Creating emitter");
        Emitter lowLevelEmitter = new NettyEmitter(topology);
        
        Listener listener = null;
        Emitter emitter = null;
        
        if (queueSize > 0) {
            logger.info("Using queueing listener and emitter");
            listener = new QueueingListener(lowLevelListener, queueSize);
            ((QueueingListener)listener).start();
            emitter = new QueueingEmitter(lowLevelEmitter, queueSize);
            ((QueueingEmitter)emitter).start();
        }
        else {
            logger.info("Using non-queueing listener and emitter");
            listener = lowLevelListener;
            emitter = lowLevelEmitter;
        }
     
        Node node = new Node(emitter, listener);
        node.run();
    }
}