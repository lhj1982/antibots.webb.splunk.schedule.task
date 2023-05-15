package org.nike.bot.webb;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.util.OutputTag;

import java.util.Properties;


/**
 * Hello world!
 *
 */
public class BotWebbFlinkApp{

    private static final ObjectMapper jsonParser = new ObjectMapper();
    private static final String region = "cn-northwest-1";
    private static final String inputStreamName = "bot-webb-splunk-kinesis";


    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName,
                new SimpleStringSchema(), inputProperties));
    }


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<DataRecord> input = createSourceFromStaticConfig(env).map(data -> jsonParser.readValue(data, DataRecord.class)).name("bot-webb-splunk-kinesis source").rebalance();
        input.addSink(new DynamicFirehoseSink());

        env.execute("Data Transporter");
    }
}
