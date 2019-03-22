//package com.datastax.nifi.processors.AutoLoader;
//
//import com.datastax.driver.core.Session;
//import com.datastax.oss.driver.api.core.type.DataTypes;
//import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
//import org.apache.commons.csv.CSVParser;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Set;
//
//import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;
//
///**
// * Created by alexandergauthier on 1/4/18.
// */
//public class TableCreator {
//    List<String> columns;
//
//    public List<String> getColumns() {
//        return columns;
//    }
//
//    public TableCreator() {
//        columns = new ArrayList<String>();
//    }
//
//    public void create(Session session, String keyspace, String tableName, CSVParser parser) throws Exception {
//
////        StringTokenizer st = new StringTokenizer(header, delimitier);
////        while (st.hasMoreTokens()) {
////            String column = st.nextToken();
////            column = column.replaceAll("[\\uFEFF\\uD83D\\uFFFD\\uFE0F\\u203C\\u3010\\u3011\\u300A\\u166D\\u200C\\u202A\\u202C\\u2049\\u20E3\\u300B\\u300C\\u3030\\u065F\\u0099\\u0F3A\\u0F3B\\uF610\\uFFFC]", "");
////
////            if (!StringUtils.isAlphanumeric(column)) {
////                column = column.replace(" ", "");
////                column = column.replace("/", "");
////                column = column.replace("_", "");
////                column = column.replace("$", "");
////                column = column.replace("#", "");
////            }
////            this.columns.add(column);
////        }
//
////
////        String strColumns = "";
////        while (it.hasNext()) {
////            strColumns += String.format("%s text, \n", it.next());
////        }
//
//        Set<String> columns = parser.getHeaderMap().keySet();
//
//        CreateTable createTable =
//                createTable(keyspace,tableName)
//                        .withPartitionKey("\"" + columns.iterator().next() + "\"", DataTypes.TEXT);
//
//        for (String columnName : columns) {
//           createTable.withColumn("\"" + columnName + "\"", DataTypes.TEXT);
//        }
//
//        session.execute(createTable.build().getQuery());
//    }
//}
