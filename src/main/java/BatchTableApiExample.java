import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class BatchTableApiExample {
    public static void main(String[] args) throws Exception {
        // Set up the batch environment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inBatchMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // Register a CSV table source
        String csvInputPath = "src/main/resources/data-in/timestamp.txt";
        tableEnv.executeSql(
                "CREATE TABLE InputTable (" +
                        "  id INT," +
                        "  name STRING," +
                        "  age INT" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'path' = '" + csvInputPath + "'," +
                        "  'format' = 'json'" +
                        ")"
        );

        // Define a simple query on the table
        Table resultTable = tableEnv.sqlQuery("SELECT id, name FROM InputTable WHERE age > 30");

        // Execute the query and collect the results
        TableResult result = resultTable.execute();

        // Print the results to the console
        result.print();

        // Write the result to a CSV file
        String csvOutputPath = "src/main/resources/data-out/output.txt";
        tableEnv.executeSql(
                "CREATE TABLE OutputTable (" +
                        "  id INT," +
                        "  name STRING" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'path' = '" + csvOutputPath + "'," +
                        "  'format' = 'json'" +
                        ")"
        );

        resultTable.executeInsert("OutputTable").collect();
    }
}
