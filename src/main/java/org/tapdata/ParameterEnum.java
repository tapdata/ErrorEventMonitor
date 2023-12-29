package org.tapdata;

public enum ParameterEnum {
    TAPDATA_WORK_DIR(0),
    TAPDATA_MONGODB_URI(1),
    TAPDARA_DATABASE(2);

    private Integer value;

    // 构造方法
    private ParameterEnum(Integer value) {
        this.value = value;
    }
    public Integer getValue() {
        return value;
    }
}
