package com.av.bigdata.storm.efd.domain;

public enum ActionInfoFields {
    EMAIL("email"), IP("ip"), ACTION_TYPE("actionType"), TIMESTAMP("timestamp");

    private String fieldName;

    ActionInfoFields(String fieldName) {
        this.fieldName = fieldName;
    }

    public static String[] fieldNames() {
        return new String[] {EMAIL.fieldName, IP.fieldName, ACTION_TYPE.fieldName, TIMESTAMP.fieldName};
    }
}
