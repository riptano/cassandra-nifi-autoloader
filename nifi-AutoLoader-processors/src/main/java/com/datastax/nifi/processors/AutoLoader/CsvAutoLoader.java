package com.datastax.nifi.processors.AutoLoader;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

/**
 * Created by alex@exalake.com on 1/4/18.
 */
public class CsvAutoLoader {

    static final int MAX_BATCH_SIZE = 1000;
    static final Logger logger = LoggerFactory.getLogger(CsvAutoLoader.class);

    DseCluster cluster;

    CSVFormat csvFormat = CSVFormat.RFC4180
            .withIgnoreEmptyLines()
            .withFirstRecordAsHeader();

    CSVParser parser;

    List<String> cleanColumns = new ArrayList<String>();


    public CsvAutoLoader(InputStream in, String... addresses) {
        DseSession session = null;

        try {
            cluster = DseCluster.builder()
                                .addContactPoints(addresses)
                                .withSocketOptions(new SocketOptions().setKeepAlive(true))
                                .withCompression(ProtocolOptions.Compression.SNAPPY)
                                .build();

            session = cluster.connect();

            Row row = session.execute("select release_version from system.local").one();
            logger.info(row.getString("release_version"));

            parser = CSVParser.parse(in, Charset.defaultCharset(), csvFormat);

            Set<String> columns = parser.getHeaderMap().keySet();
            for (String column : columns) {
                column = column.replaceAll("[\\uFEFF\\uD83D\\uFFFD\\uFE0F\\u203C\\u3010\\u3011\\u300A\\u166D" +
                        "\\u200C\\u202A\\u202C\\u2049\\u20E3\\u300B\\u300C\\u3030\\u065F\\u0099\\u0F3A\\u0F3B" +
                        "\\uF610\\uFFFC]", "");

                column = column.replaceAll("[^a-zA-Z]", "").toLowerCase();

                cleanColumns.add(column);
            }

        } catch (Exception e) {

        }
        finally {
            session.close();
        }
    }

    public void createTableFromHeader(String keyspace, String tableName) throws Exception {
        DseSession session = cluster.connect();

        CreateTable createTableStat = createTable(keyspace, tableName)
                .withPartitionKey( cleanColumns.iterator().next(), DataTypes.TEXT);

        for (String columnName : cleanColumns) {
            createTableStat = createTableStat.withColumn(columnName, DataTypes.TEXT);
        }

        try {
            session.execute(createTableStat.asCql());
        } catch (Exception ex) {

        } finally {
            session.close();
        }
    }

    public void loadTable(String keyspace, String table) throws Exception {

        // Connect
        DseSession dseSession = cluster.connect();
        BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);


        for (CSVRecord csvRecord : parser) {

            List<Object> values = new ArrayList<Object>();

            List<String> csvColumn = new ArrayList<String>(parser.getHeaderMap().keySet());

            for (String column: csvColumn) {
                values.add(csvRecord.get(column));
            }

            Insert insert =
                    QueryBuilder.insertInto(keyspace, table)
                    .values(new ArrayList<String>(cleanColumns), values);

            batch.add(insert);

            if (batch.size() >= MAX_BATCH_SIZE) {
                dseSession.execute(batch);
                batch.clear();
            }
        }

        // final batch
        if (batch.size() > 0) {
            dseSession.execute(batch);
        }

        dseSession.close();
        cluster.close();
    }
}