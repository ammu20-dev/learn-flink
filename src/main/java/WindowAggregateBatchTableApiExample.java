import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class WindowAggregateBatchTableApiExample {
    public static void main(String[] args) throws Exception {
        // Set up the batch environment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inBatchMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // Register a CSV table source
        String csvInputPath = "src/main/resources/data-in/aggregate.txt";
        tableEnv.executeSql(
                "CREATE TABLE InputTable (" +
                        "  bidtime TIMESTAMP(3)," +
                        "  price DECIMAL(10,2)," +
                        "  item STRING," +
                        "  supplier_id STRING" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'path' = '" + csvInputPath + "'," +
                        "  'format' = 'json'" +
                        ")"
        );

        //Describe table and print
        TableResult descTable = tableEnv.executeSql("desc InputTable");
        descTable.print();
        // Define a simple query on the table
        Table resultTable = tableEnv.sqlQuery("SELECT window_start, window_end, SUM(price) AS total_price\n" +
                "  FROM TABLE(\n" +
                "    TUMBLE(TABLE InputTable, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))\n" +
                "  GROUP BY window_start, window_end");
        //working
       /* Table resultTable = tableEnv.sqlQuery("SELECT TUMBLE_START(bidtime, INTERVAL '10' MINUTES) AS window_start,\n" +
                "    TUMBLE_END(bidtime, INTERVAL '10' MINUTES) AS window_end, SUM(price) AS total_price\n" +
                "  FROM InputTable\n" +
                "  GROUP BY TUMBLE(bidtime, INTERVAL '10' MINUTES)");*/
       //testing
       /* Table resultTable = tableEnv.sqlQuery("SELECT window_start,\n" +
                "  window_end, SUM(price) AS total_price\n" +
                "  FROM TABLE(\n" +
                "  TUMBLE(TABLE InputTable, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))\n" +
                "  GROUP BY window_start, window_end");*/

        // Execute the query and collect the results
        TableResult result = resultTable.execute();

        // Print the results to the console
        result.print();

        // Write the result to a CSV file
        String csvOutputPath = "src/main/resources/data-out/output.txt";
        /*tableEnv.executeSql(
                "CREATE TABLE OutputTable (" +
                        "  id INT," +
                        "  name STRING" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'path' = '" + csvOutputPath + "'," +
                        "  'format' = 'json'" +
                        ")"
        );

        resultTable.executeInsert("OutputTable").collect();*/
    }
}
