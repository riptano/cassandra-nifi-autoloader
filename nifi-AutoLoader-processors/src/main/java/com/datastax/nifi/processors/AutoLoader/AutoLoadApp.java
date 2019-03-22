/*
 * Copyright (c) 2019.
 */

package com.datastax.nifi.processors.AutoLoader;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.util.Arrays;



public class AutoLoadApp {
    static final Logger logger = LoggerFactory.getLogger(AutoLoadApp.class);

    private static Options generateOptions() {
        final Option verboseOption = Option.builder("f")
                .required(true)
                .hasArg(true)
                .longOpt("file")
                .desc("path to csv file")
                .build();
        final Option fileOption = Option.builder("d")
                .required()
                .longOpt("delimiter")
                .hasArg()
                .desc("Delimiter")
                .build();
        final Option address = Option.builder("a")
                .required()
                .longOpt("addresss")
                .hasArg()
                .desc("address")
                .build();
        final Option keySpaceOption = Option.builder("k")
                .required()
                .longOpt("keyspace")
                .hasArg()
                .desc("key space")
                .build();
        final Option tableName = Option.builder("t")
                .required()
                .longOpt("tableName")
                .hasArg()
                .desc("table name")
                .build();

        final Options options = new Options();
        options.addOption(verboseOption);
        options.addOption(fileOption);
        options.addOption(address);
        options.addOption(keySpaceOption);
        options.addOption(tableName);
        return options;
    }

    private static CommandLine generateCommandLine(
            final Options options, final String[] commandLineArguments) {
        final CommandLineParser cmdLineParser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = cmdLineParser.parse(options, commandLineArguments);
        } catch (ParseException parseException) {
            logger.error("ERROR: Unable to parse command-line arguments "
                            + Arrays.toString(commandLineArguments) + " due to: "
                            + parseException);
        }
        return commandLine;
    }

    public static int main(String[] args) {
        CommandLine cmdLine = AutoLoadApp.generateCommandLine(AutoLoadApp.generateOptions(), args);
        String filename = cmdLine.getOptionValue("f");
        String delimiter = cmdLine.getOptionValue("d");
        String keyspace = cmdLine.getOptionValue("k");
        String table = cmdLine.getOptionValue("t");
        String[] addresses = {cmdLine.getOptionValue("a")};

        try {
            InputStream is = new FileInputStream(filename);
            CsvAutoLoader csvAutoLoader = new CsvAutoLoader(is, addresses);
            csvAutoLoader.createTableFromHeader(keyspace,table);
            csvAutoLoader.loadTable(keyspace, table);

        } catch (Exception e) {
            logger.error(e.getMessage());
            return -1;
        }

        return 0;
    }
}
