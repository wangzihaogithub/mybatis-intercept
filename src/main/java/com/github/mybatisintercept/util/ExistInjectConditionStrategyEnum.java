package com.github.mybatisintercept.util;

/**
 * 注入的条件已存在时的处理策略枚举
 */
public enum ExistInjectConditionStrategyEnum {
    /**
     * 任意表里存在条件, 就跳过整个SQL
     */
    ANY_TABLE_MATCH_THEN_SKIP_SQL,
    /**
     * 没被配置文件排除的表里，如果存在条件, 就跳过整个SQL
     */
    RULE_TABLE_MATCH_THEN_SKIP_SQL,
    /**
     * 没被配置文件排除的表里，如果存在条件, 就跳过当前子条件项
     */
    RULE_TABLE_MATCH_THEN_SKIP_ITEM,
    /**
     * 永远追加注入的条件
     */
    ALWAYS_APPEND
}