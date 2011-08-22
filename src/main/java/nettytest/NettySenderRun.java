package nettytest;

import io.Emitter;
import io.QueueingEmitter;

import java.io.File;
import java.io.Reader;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import nettytest.NettyEmitter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import topology.Topology;
import topology.TopologyFromFile;
import generictest.Sender;

public class NettySenderRun {
    public static Logger logger = Logger.getLogger(NettySenderRun.class);
    
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
            logger.error("No message count specified");
            System.exit(1);
        }

        int messageCount = -1;
        try {
            messageCount = Integer.parseInt((String) loArgs.get(0));
        } catch (NumberFormatException nfe) {
            logger.error(String.format("Bad message count specified: %s",
                    loArgs.get(0)));
            System.exit(1);
        }

        if (loArgs.size() < 2) {
            logger.error("No rate specified");
            System.exit(1);

        }

        int rate = -1;
        try {
            rate = Integer.parseInt((String) loArgs.get(1));
        } catch (NumberFormatException nfe) {
            logger.error(String.format("Bad rate specified: %s", loArgs.get(1)));
            System.exit(1);
        }
        
        if (loArgs.size() < 3) {
            logger.error("No cluster name specified");
            System.exit(1);
        }

        String clusterName = (String)loArgs.get(2);     

        if (loArgs.size() < 4) {
            logger.error("No partition map file specified");
            System.exit(1);
        }

        String configFilename = (String)loArgs.get(3);
        if (!(new File(configFilename)).exists()) {
            logger.error(String.format("Specified configuration file %s does not exist", configFilename));
        }

        if (loArgs.size() < 5) {
            logger.error("No string file specified");
            System.exit(1);
        }

        File stringFile = new File((String) loArgs.get(4));
        if (!stringFile.exists()) {
            logger.error(String.format("Bad string file specified: %s", loArgs.get(3)));
            System.exit(1);
        }
        
        Reader r = null;
        BufferedReader stringReader = null;
        List<String> randomStrings = new ArrayList<String>();
        try {
            r = new FileReader(stringFile);
            stringReader = new BufferedReader(r);
            String line = null;
            while ((line = stringReader.readLine()) != null) {
                if (line.trim().length() == 0) {
                    continue;
                }
                randomStrings.add(line);
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(1);
        } finally {
            try {
                stringReader.close();
            } catch (Exception e) {
            }
            try {
                r.close();
            } catch (Exception e) {
            }
        }

        Topology topology = new TopologyFromFile(clusterName, configFilename);
        logger.info("Creating emitter");
        Emitter lowLevelEmitter = new NettyEmitter(topology);
        Emitter emitter = null;
        if (queueSize > 0) {
            logger.info("Using queueing emitter");
            emitter = new QueueingEmitter(lowLevelEmitter, queueSize);
            ((QueueingEmitter)emitter).start();
        }
        else {
            logger.info("Using non-queueing emitter");
            emitter = lowLevelEmitter;
        }
        
        try {
            Thread.sleep(5000); // allow for all connections to be made
        }
        catch (InterruptedException ie) {}
        
        String[] randomStringsArray = new String[randomStrings.size()];
        Sender sender = new Sender(emitter, messageCount, rate, randomStrings.toArray(randomStringsArray));
        sender.run();
        
        try {
            Thread.sleep(99999999);
        } catch (InterruptedException ie) {
        }
    }
}