package com.antontech.itemkafka_poc.model;

/** Request body for {@code POST /item-kafka/consumer/manual-consume/v1}. */
public class ManualConsumeRequest {
    private String groupId;
    private String msg;

    /** @return the Kafka consumer group id to poll with ({@code item_group} or {@code manual-item-group}). */
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /** @return an optional free-text message accompanying the request (informational only). */
    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}

