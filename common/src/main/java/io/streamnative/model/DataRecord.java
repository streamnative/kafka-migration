package io.streamnative.model;

public class DataRecord {

    Long count;

    public DataRecord() {
    }

    public DataRecord(Long count) {
        this.count = count;
    }

    public Long getCount() {
        return count;
    }

    @Override
    public String toString() {
        return new com.google.gson.Gson().toJson(this);
    }
}
