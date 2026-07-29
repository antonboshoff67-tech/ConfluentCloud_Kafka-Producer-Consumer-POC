package za.co.woolworths.itemkafka_poc.model;

public class ManualConsumeRequest {
    private String groupId;
    private String msg;

    // Getters and Setters
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
