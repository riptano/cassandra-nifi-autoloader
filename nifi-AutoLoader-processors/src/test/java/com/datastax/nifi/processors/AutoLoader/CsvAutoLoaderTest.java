package com.datastax.nifi.processors.AutoLoader;
import org.junit.Test;
import java.util.Arrays;

/**
 * Created by alexandergauthier on 1/5/18.
 */
public class CsvAutoLoaderTest {
    public static final String PATH = "/Users/cleaner/data/master.csv";
    public static final String DELIM = ",";
    public static final String ADDR = "10.200.178.17";
    public static final String KEYSPACE = "staging";
    public static final String TABLE = "master_test";

    @Test
    public void main() throws Exception {
        //String tableName = String.format("t_%s",  UUID.randomUUID().toString()).replace("-","");
        String args[] = {"-f", PATH, "-d", DELIM, "-a", ADDR, "-k", KEYSPACE, "-t", TABLE};
        System.out.println(Arrays.toString(args));
        AutoLoadApp.main(args);
    }
}