package org.nike.bot.webb;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataRecord {
    private String type;
    private String value;
    private String author;
    private String ttl;
    private String action;
    private String nameSpace;
    private String taskId;

    @JsonIgnore
    private List<Map<String, List<String>>> metadata;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static class DataRecordsSerializationSchema implements SerializationSchema<DataRecord>
    {
        private static final long serialVersionUID = 1L;
        @Override
        public byte[] serialize(DataRecord dataRecord)
        {
            return dataRecord.toString().getBytes();
        }
    }

    public static DataRecordsSerializationSchema sinkSerializer()
    {
        return new DataRecordsSerializationSchema();
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getTtl() {
        return ttl;
    }

    public void setTtl(String ttl) {
        this.ttl = ttl;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    public List<Map<String, List<String>>> getMetadata() {
        return metadata;
    }

    public String getTaskId(){ return taskId;}

    public void setTaskId(String taskId) { this.taskId = taskId; }

    public void setMetadata(List<Map<String, List<String>>> metadata) {
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return "{" +
                "\"type\":\"" + type + '\"' +
                ", \"value\":\"" + value + '\"' +
                ", \"author\":\"" + author + '\"' +
                ", \"ttl\":\"" + ttl + '\"' +
                ", \"action\":\"" + action + '\"' +
                ", \"nameSpace\":\"" + nameSpace + '\"' +
                ", \"taskId\":\"" + taskId + '\"' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataRecord that = (DataRecord) o;

        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;
        if (author != null ? !author.equals(that.author) : that.author != null) return false;
        if (ttl != null ? !ttl.equals(that.ttl) : that.ttl != null) return false;
        if (action != null ? !action.equals(that.action) : that.action != null) return false;
        if (nameSpace != null ? !nameSpace.equals(that.nameSpace) : that.nameSpace != null) return false;
        if (taskId != null ? !taskId.equals(that.taskId) : that.taskId != null) return false;
        return metadata != null ? metadata.equals(that.metadata) : that.metadata == null;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (author != null ? author.hashCode() : 0);
        result = 31 * result + (ttl != null ? ttl.hashCode() : 0);
        result = 31 * result + (action != null ? action.hashCode() : 0);
        result = 31 * result + (nameSpace != null ? nameSpace.hashCode() : 0);
        result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
        result = 31 * result + (taskId != null ? taskId.hashCode() : 0);
        return result;
    }
}
