package org.nike.bot.webb;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchRequest;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponse;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponseEntry;
import software.amazon.awssdk.services.firehose.model.Record;

import java.util.*;

public class DynamicFirehoseSink extends RichSinkFunction<DataRecord> {
    public static Logger LOG = LoggerFactory.getLogger(DynamicFirehoseSink.class);

    private transient FirehoseClient firehoseClient;
    private transient Map<String, List<Record>> buffer;

    private static final int BATCH_SIZE = 500; // Adjust this value based on your requirements, only 500 and below 500 is acceptable or else it will throw 400

    @Override
    public void open(Configuration parameters) {
        firehoseClient = FirehoseClient.builder()
                .region(Region.CN_NORTHWEST_1)
                .build();

        buffer = new HashMap<>();
    }

    @Override
    public void invoke(DataRecord value, Context context) {
        if ( value.getMetadata() != null && value.getMetadata().size() > 0 ){
            List<String> destinations = value.getMetadata().get(0).get("destination");

            for (String destination : destinations) {
                String firehoseStreamName = "bot-webb-splunk-firehose-" + destination.toLowerCase();

                Record record = Record.builder()
                        .data(SdkBytes.fromByteArray(value.toByteArray()))
                        .build();

                buffer.putIfAbsent(firehoseStreamName, new ArrayList<>());
                buffer.get(firehoseStreamName).add(record);

                if (buffer.get(firehoseStreamName).size() >= BATCH_SIZE) {
                    flush(firehoseStreamName);
                }
            }
        }
    }

    private void flush(String firehoseStreamName) {
        List<Record> records = buffer.get(firehoseStreamName);

        while (!records.isEmpty()) {
            try {
                PutRecordBatchResponse putRecordBatchResponse = sendRecords(firehoseStreamName, records);
                if (putRecordBatchResponse.failedPutCount() != 0){
                    List<Record> resendList = new ArrayList<>();
                    List<PutRecordBatchResponseEntry> responsesList = putRecordBatchResponse.requestResponses();
                    for (int i=0;i < responsesList.size(); i++){
                        if(responsesList.get(i).errorCode() != null && responsesList.get(i).errorCode() != ""){
                            LOG.info("data failed to send: error_code {}, error_message {}",responsesList.get(i).errorCode(),responsesList.get(i).errorMessage());
                            resendList.add(records.get(i));
                        }
                    }
                    records.clear();
                    records = resendList;
                }else {
                    records.clear();
                }
            }catch (Exception e){
                LOG.error("Exception caught from catch: {}",e.getMessage());
            }
        }
        buffer.put(firehoseStreamName,records);
    }

    private PutRecordBatchResponse sendRecords(String firehoseStreamName,List<Record> records) {
        PutRecordBatchRequest putRecordBatchRequest = PutRecordBatchRequest.builder()
                .deliveryStreamName(firehoseStreamName)
                .records(records)
                .build();
        return firehoseClient.putRecordBatch(putRecordBatchRequest);
    }

    @Override
    public void close() {
        if (firehoseClient != null) {
            firehoseClient.close();
        }
    }
}