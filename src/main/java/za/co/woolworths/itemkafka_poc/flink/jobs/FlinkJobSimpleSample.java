package za.co.woolworths.itemkafka_poc.flink.jobs;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class FlinkJobSimpleSample
{

    public static void main(String[] args) throws Exception
    {
        // At the start of the job
        log.debug("Starting Simple Flink Job Example"); // ###
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*Configuration flinkConfig = new Configuration();
        flinkConfig.setString(TaskManagerOptions.TOTAL_PROCESS_MEMORY.key(), "1024m");
        flinkConfig.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), "512m");
        flinkConfig.set(TaskManagerOptions.NUM_TASK_SLOTS, 2);*/

        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        //env.getConfig().setGlobalJobParameters(flinkConfig);

        List<String> elements = Arrays.asList("Hello", "World", "Flink");
        DataStream<String> stream = env.fromData(elements);

        // Log the data stream
        stream.map(new MapFunction<String, String>()
        {
            @Override
            public String map(String value) throws Exception
            {
                log.debug("Original Value: {}", value);
                return value;
            }
        });

        DataStream<String> processedStream = stream.map(new MapFunction<String, String>()
        {
            @Override
            public String map(String value) throws Exception
            {
                String result = "Processed: " + value;
                log.debug("Processed Value: {}", result);
                return result;
            }
        });

        processedStream.print();

        try
        {
            env.execute("Simple Flink Job Example");
            log.debug("Simple Flink Job executed successfully.");
        }
        catch (Exception e)
        {
            log.error("Execution failed for Simple Flink Job: {}", e.getMessage(), e);
        }
    }
}
