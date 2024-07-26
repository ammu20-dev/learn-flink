import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.time.ZoneId;

public final class SavePointExample {

    public static void main(String[] args) {

        final String JOB_NAME = "FlinkJob";

        final EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        final TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.getConfig().set("pipeline.name", JOB_NAME);
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));

        tEnv.executeSql("CREATE TEMPORARY TABLE ApiLog (" +
                "  `_timestamp` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL," +
                "  `_partition` INT METADATA FROM 'partition' VIRTUAL," +
                "  `_offset` BIGINT METADATA FROM 'offset' VIRTUAL," +
                "  `Data` STRING," +
                "  `Action` STRING," +
                "  `ProduceDateTime` TIMESTAMP_LTZ(6)," +
                "  `OffSet` INT" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'api.log'," +
                "  'properties.group.id' = 'flink'," +
                "  'properties.bootstrap.servers' = '<mykafkahost...>'," +
                "  'format' = 'json'," +
                "  'json.timestamp-format.standard' = 'ISO-8601'" +
                ")");

        tEnv.executeSql("CREATE TABLE print_table (" +
                " `_timestamp` TIMESTAMP(3)," +
                " `_partition` INT," +
                " `_offset` BIGINT," +
                " `Data` STRING," +
                " `Action` STRING," +
                " `ProduceDateTime` TIMESTAMP(6)," +
                " `OffSet` INT" +
                ") WITH ('connector' = 'print')");

        tEnv.executeSql("INSERT INTO print_table" +
                " SELECT * FROM ApiLog");

    }

}