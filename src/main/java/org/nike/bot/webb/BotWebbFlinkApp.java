package org.nike.bot.webb;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.firehose.sink.KinesisFirehoseSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;

public class BotWebbFlinkApp {

    private static final ObjectMapper jsonParser = new ObjectMapper();
    private static final String region = "cn-northwest-1";
    private static final String inputStreamName = "bot-webb-splunk-kinesis";
    private static final String EDGEKV_FIREHOUSE = "bot-webb-splunk-firehose-edgekv";
    private static final String FAIRNESS_FIREHOUSE = "bot-webb-splunk-firehose-fairness";
    private static final String ATHENA_FIREHOUSE = "bot-webb-splunk-firehose-athena";
    private static final String EDGEKV = "edgeKV";
    private static final String FAIRNESS = "fairness";
    private static final String ATHENA = "athena";
    public static Logger LOG = LoggerFactory.getLogger(BotWebbFlinkApp.class);

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

        KinesisFirehoseSink<String> sinkAthena = KinesisFirehoseSink.<String>builder()
                .setFirehoseClientProperties(sinkProperties)
                .setSerializationSchema(new SimpleStringSchema())
                .setDeliveryStreamName(ATHENA_FIREHOUSE)
                .build();

        DataStream<DataRecord> input = env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties))
                .map(data -> jsonParser.readValue(data, DataRecord.class))
                .name("bot-webb-splunk-kinesis source")
                .name("Sourcing Data from KDS")
                .rebalance();

        input.keyBy(DataRecord::getTaskId)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)))
                .aggregate(new TaskIdLoggerFunction()).name("TaskID Logger");

        DataStream<String> edgeKvDs = input.flatMap((DataRecord dataRecord, Collector<String> out) -> {
            if (dataRecord.getMetadata().get(0) != null && dataRecord.getMetadata().get(0).size() > 0) {
                List<String> destination = dataRecord.getMetadata().get(0).get("destination");
                if (destination.contains(EDGEKV)) out.collect("{" +
                        "\"type\":\"" + dataRecord.getType() + '\"' +
                        ", \"value\":\"" + dataRecord.getValue() + '\"' +
                        ", \"action\":\"" + dataRecord.getAction() + '\"' +
                        ", \"nameSpace\":\"" + dataRecord.getNameSpace() + '\"' +
                        ", \"ttl\":\"" + dataRecord.getTtl() + '\"' +
                        ", \"taskId\":\"" + dataRecord.getTaskId() + '\"' +
                        ", \"ruleId\":\"" + dataRecord.getRuleId() + '\"' +
                        ", \"versionId\":\"" + dataRecord.getVersionId() + '\"' +
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
                        ", \"taskId\":\"" + dataRecord.getTaskId() + '\"' +
                        ", \"ruleId\":\"" + dataRecord.getRuleId() + '\"' +
                        ", \"versionId\":\"" + dataRecord.getVersionId() + '\"' +
                        '}');
            }
        }).returns(Types.STRING);

        DataStream<String> athenaDs = input.flatMap((DataRecord dataRecord, Collector<String> out) -> {
            if (dataRecord.getMetadata().get(0) != null && dataRecord.getMetadata().get(0).size() > 0) {
                List<String> destination = dataRecord.getMetadata().get(0).get("destination");
                if (destination.contains(ATHENA)) out.collect("{" +
                        "\"type\":\"" + dataRecord.getType() + '\"' +
                        ", \"value\":\"" + dataRecord.getValue() + '\"' +
                        ", \"author\":\"" + dataRecord.getAuthor() + '\"' +
                        ", \"ttl\":\"" + dataRecord.getTtl() + '\"' +
                        ", \"taskId\":\"" + dataRecord.getTaskId() + '\"' +
                        ", \"ruleId\":\"" + dataRecord.getRuleId() + '\"' +
                        ", \"versionId\":\"" + dataRecord.getVersionId() + '\"' +
                        '}');
            }
        }).returns(Types.STRING);

        fairnessDs.sinkTo(sinkFairness).name("Sink to Fairness");
        edgeKvDs.sinkTo(sinkEdgeKV).name("Sink to EdgeKV");
        athenaDs.sinkTo(sinkAthena).name("Sink to Athena");

        env.execute("Kinesis to Flink to Firehose App");
    }
}

