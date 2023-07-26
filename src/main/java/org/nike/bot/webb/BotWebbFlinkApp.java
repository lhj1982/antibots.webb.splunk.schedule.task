package org.nike.bot.webb;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.firehose.sink.KinesisFirehoseSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.util.Collector;

import java.util.List;
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

        KinesisFirehoseSink<String> sinkEdgeKV = KinesisFirehoseSink.<String>builder()
                .setFirehoseClientProperties(sinkProperties)
                .setSerializationSchema(new SimpleStringSchema())
                .setDeliveryStreamName(EDGEKV_FIREHOUSE)
                .build();

        KinesisFirehoseSink<String> sinkFairness = KinesisFirehoseSink.<String>builder()
                .setFirehoseClientProperties(sinkProperties)
                .setSerializationSchema(new SimpleStringSchema())
                .setDeliveryStreamName(FAIRNESS_FIREHOUSE)
                .build();

        DataStream<DataRecord> input = env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties))
                .map(data -> jsonParser.readValue(data, DataRecord.class))
                .name("bot-webb-splunk-kinesis source")
                .rebalance();


        DataStream<String> edgeKvDs = input.flatMap((DataRecord dataRecord, Collector<String> out)->{
            if (dataRecord.getMetadata().get(0) != null && dataRecord.getMetadata().get(0).size() > 0) {
                List<String> destination = dataRecord.getMetadata().get(0).get("destination");
                if (destination.contains(EDGEKV)) out.collect("{" +
                                                                        "\"type\":\"" + dataRecord.getType() + '\"' +
                                                                        ", \"value\":\"" + dataRecord.getValue() + '\"' +
                                                                        ", \"action\":\"" + dataRecord.getAction() + '\"' +
                                                                        ", \"nameSpace\":\"" + dataRecord.getNameSpace() + '\"' +
                                                                        ", \"taskId\":\"" + dataRecord.getTaskId() + '\"' +
                                                                        '}');
            }
        }).returns(Types.STRING);

        DataStream<String> fairnessDs = input.flatMap((DataRecord dataRecord, Collector<String> out) -> {
            if (dataRecord.getMetadata().get(0) != null && dataRecord.getMetadata().get(0).size() > 0) {
                List<String> destination = dataRecord.getMetadata().get(0).get("destination");
                if (destination.contains(FAIRNESS)) out.collect("{" +
                                                                        "\"type\":\"" + dataRecord.getType() + '\"' +
                                                                        ", \"value\":\"" + dataRecord.getValue() + '\"' +
                                                                        ", \"author\":\"" + dataRecord.getAuthor() + '\"' +
                                                                        ", \"ttl\":\"" + dataRecord.getTtl() + '\"' +
                                                                        '}');
            }
        }).returns(Types.STRING);


        fairnessDs.sinkTo(sinkFairness);
        edgeKvDs.sinkTo(sinkEdgeKV);


        env.execute("Kinesis to Flink to Firehose App");
    }
}

