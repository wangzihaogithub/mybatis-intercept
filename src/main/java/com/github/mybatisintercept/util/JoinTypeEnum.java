package com.github.mybatisintercept.util;

public enum JoinTypeEnum {
    LEFT_OUTER_JOIN("LEFT JOIN"), //
    COMMA(","), //
    INNER_JOIN("INNER JOIN"), //
    RIGHT_OUTER_JOIN("RIGHT JOIN"), //
    JOIN("JOIN"), //
    CROSS_JOIN("CROSS JOIN"), //
    NATURAL_CROSS_JOIN("NATURAL CROSS JOIN"), //
    NATURAL_JOIN("NATURAL JOIN"), //
    NATURAL_INNER_JOIN("NATURAL INNER JOIN"), //
    LEFT_SEMI_JOIN("LEFT SEMI JOIN"), //
    LEFT_ANTI_JOIN("LEFT ANTI JOIN"), //
    FULL_OUTER_JOIN("FULL JOIN"),//
    STRAIGHT_JOIN("STRAIGHT_JOIN"), //
    OUTER_APPLY("OUTER APPLY"),//
    CROSS_APPLY("CROSS APPLY"),
    UNKNOWN("UNKNOWN");

    private final String code;

    JoinTypeEnum(String code) {
        this.code = code;
    }

    public static JoinTypeEnum codeOf(String code) {
        for (JoinTypeEnum value : values()) {
            if (value.name().equals(code)) {
                return value;
            }
        }
        for (JoinTypeEnum value : values()) {
            if (value.code.equals(code)) {
                return value;
            }
        }
        return UNKNOWN;
    }
}