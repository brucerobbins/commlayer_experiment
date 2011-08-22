package udptest;

import generictest.Node;
import io.Emitter;
import io.Listener;

import java.io.File;
import java.io.Reader;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import nettytest.NettyEmitter;
import nettytest.NettyListener;

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
import udptest.UDPListener;

public class UDPNodeRun {
    public static Logger logger = Logger.getLogger(UDPNodeRun.class);

    public static void main(String[] args) {
        BasicConfigurator.configure();
        
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("udpbuffersize").hasArg()
                .withDescription("UDP Buffer Size").create("u"));

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

        int udpBufferSize = -1;
        if (commandLine.hasOption("u")) {
            try {
                udpBufferSize = Integer.parseInt(commandLine
                        .getOptionValue("u"));
            } catch (NumberFormatException e) {
                logger.error("Bad queue "
                        + commandLine.getOptionValue("u"));
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
        Listener listener = new UDPListener(assignment, udpBufferSize);
        logger.info("Creating emitter");
        Emitter emitter = new UDPEmitter(topology);
        
        Node node = new Node(emitter, listener);
        node.run();
    }
}