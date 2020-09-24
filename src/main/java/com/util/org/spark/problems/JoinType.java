package com.util.org.spark.problems;

public enum JoinType {
    LEFT("left"),
    RIGHT("right"),
    INNER("inner"),
    FULL("full");

    private String joinVal;

    JoinType(String joinType) {
        joinVal = joinType;
    }

    public String getJoinVal() {
        return joinVal;
    }
}
