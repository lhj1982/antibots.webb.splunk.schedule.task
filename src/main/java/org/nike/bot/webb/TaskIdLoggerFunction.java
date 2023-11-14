package org.nike.bot.webb;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class TaskIdLoggerFunction implements AggregateFunction<DataRecord, Set<String>, Void> {
    public static Logger LOG = LoggerFactory.getLogger(TaskIdLoggerFunction.class);

    @Override
    public Set<String> createAccumulator() {
        return new HashSet<>();
    }

    @Override
    public Set<String> add(DataRecord value, Set<String> taskIds) {
        taskIds.add(value.getTaskId());
        return taskIds;
    }

    @Override
    public Void getResult(Set<String> taskIds) {
        for (String taskID : taskIds) {
            LOG.info("Processed taskId: {}", taskID);
        }
        return null;
    }

    @Override
    public Set<String> merge(Set<String> a, Set<String> b) {
        return null;
    }
}
