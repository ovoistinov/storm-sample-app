package com.av.bigdata.storm.efd.domain;

public class ActionInfo {
    private String emailAddress;
    private String ip;
    private ActionType actionType;
    private long timestamp;

    public ActionInfo(String emailAddress, String ip, ActionType actionType, long timestamp) {
        this.emailAddress = emailAddress;
        this.ip = ip;
        this.actionType = actionType;
        this.timestamp = timestamp;
    }

    public String getEmailAddress() {
        return emailAddress;
    }

    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public ActionType getActionType() {
        return actionType;
    }

    public void setActionType(ActionType actionType) {
        this.actionType = actionType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "ActionInfo [emailAddress=" + emailAddress + ", ip=" + ip + ", actionType=" + actionType + ", timestamp=" + timestamp + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((actionType == null) ? 0 : actionType.hashCode());
        result = prime * result + ((emailAddress == null) ? 0 : emailAddress.hashCode());
        result = prime * result + ((ip == null) ? 0 : ip.hashCode());
        result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ActionInfo other = (ActionInfo) obj;
        if (actionType != other.actionType)
            return false;
        if (emailAddress == null) {
            if (other.emailAddress != null)
                return false;
        }
        else if (!emailAddress.equals(other.emailAddress))
            return false;
        if (ip == null) {
            if (other.ip != null)
                return false;
        }
        else if (!ip.equals(other.ip))
            return false;
        if (timestamp != other.timestamp)
            return false;
        return true;
    }
}
