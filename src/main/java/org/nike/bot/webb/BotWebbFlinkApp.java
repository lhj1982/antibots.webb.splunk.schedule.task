package org.nike.bot.webb;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.firehose.sink.KinesisFirehoseSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
public class BotWebbFlinkApp {

    private static final ObjectMapper jsonParser = new ObjectMapper();
    private static final String region = "cn-northwest-1";
    private static final String inputStreamName = "bot-webb-splunk-kinesis";
    private static final String EDGEKV_FIREHOUSE = "bot-webb-splunk-firehose-edgekv";
    private static final String FAIRNESS_FIREHOUSE = "bot-webb-splunk-firehose-fairness";
    private static final String EDGEKV = "edgeKV";
    private static final String FAIRNESS = "fairness";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        Properties sinkProperties = new Properties();
        sinkProperties.setProperty(AWS_REGION, region);

        KinesisFirehoseSink<DataRecord> sinkEdgeKV = KinesisFirehoseSink.<DataRecord>builder()
                .setFirehoseClientProperties(sinkProperties)
                .setSerializationSchema(DataRecord.sinkSerializer())
                .setDeliveryStreamName(EDGEKV_FIREHOUSE)
                .build();

        KinesisFirehoseSink<DataRecord> sinkFairness = KinesisFirehoseSink.<DataRecord>builder()
                .setFirehoseClientProperties(sinkProperties)
                .setSerializationSchema(DataRecord.sinkSerializer())
                .setDeliveryStreamName(FAIRNESS_FIREHOUSE)
                .build();

        DataStream<DataRecord> input = env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties))
                .map(data -> jsonParser.readValue(data, DataRecord.class))
                .name("bot-webb-splunk-kinesis source")
                .rebalance();


        DataStream<DataRecord> edgeKvDs = input.filter((FilterFunction<DataRecord>) dataRecord -> {
            if (dataRecord.getMetadata().get(0) != null && dataRecord.getMetadata().get(0).size() > 0) {
                List<String> destination = dataRecord.getMetadata().get(0).get("destination");
                return destination.contains(EDGEKV);
            } else
                return false;
        });

        DataStream<DataRecord> fairnessDs = input.filter((FilterFunction<DataRecord>) dataRecord -> {
            if (dataRecord.getMetadata().get(0) != null && dataRecord.getMetadata().get(0).size() > 0) {
                List<String> destination = dataRecord.getMetadata().get(0).get("destination");
                return destination.contains(FAIRNESS);
            } else
                return false;
        });

        fairnessDs.sinkTo(sinkFairness);
        edgeKvDs.sinkTo(sinkEdgeKV);


        env.execute("Kinesis to Flink to Firehose App");
    }
}

