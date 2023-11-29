package com.github.mybatisintercept.util;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;

import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

public class ASTDruidTestUtil {

    static String addAndCondition(String sql, String injectCondition, ASTDruidConditionUtil.ExistInjectConditionStrategyEnum existInjectConditionStrategyEnum, String dbType, BiPredicate<String, String> skip) {
        return ASTDruidConditionUtil.addCondition(sql, injectCondition, SQLBinaryOperator.BooleanAnd, false, existInjectConditionStrategyEnum, dbType, skip, null);
    }

    static String addAndConditionIgnoreUniqueKey(String sql, String injectCondition, ASTDruidConditionUtil.ExistInjectConditionStrategyEnum existInjectConditionStrategyEnum, String dbType, BiPredicate<String, String> skip, Map<String, List<String>> table) {
        return ASTDruidConditionUtil.addCondition(sql, injectCondition, SQLBinaryOperator.BooleanAnd, false, existInjectConditionStrategyEnum, dbType, skip, sqlJoin -> sqlJoin.existUniqueKeyColumn(table));
    }

    static String addAndCondition(String sql, String injectCondition, ASTDruidConditionUtil.ExistInjectConditionStrategyEnum strategyEnum) {
        return ASTDruidConditionUtil.addCondition(sql, injectCondition, SQLBinaryOperator.BooleanAnd, false,
                strategyEnum, "mysql", null, null);
    }

    static String addAndCondition(String sql, String injectCondition, ASTDruidConditionUtil.ExistInjectConditionStrategyEnum strategyEnum, Map<String, List<String>> table) {
        return ASTDruidConditionUtil.addCondition(sql, injectCondition, SQLBinaryOperator.BooleanAnd, false,
                strategyEnum, "mysql", null, sqlJoin -> sqlJoin.existUniqueKeyColumn(table));
    }

    static String addAndCondition(String sql, String injectCondition) {
        return ASTDruidConditionUtil.addCondition(sql, injectCondition, SQLBinaryOperator.BooleanAnd, false,
                ASTDruidConditionUtil.ExistInjectConditionStrategyEnum.RULE_TABLE_MATCH_THEN_SKIP_SQL, "mysql", null, null);
    }

    static String addAndConditionIgnoreUniqueKey(String sql, String injectCondition, ASTDruidConditionUtil.ExistInjectConditionStrategyEnum strategyEnum, Map<String, List<String>> table) {
        return ASTDruidConditionUtil.addCondition(sql, injectCondition, SQLBinaryOperator.BooleanAnd, false,
                strategyEnum, "mysql", null, sqlJoin -> sqlJoin.existUniqueKeyColumn(table));
    }

    static String addAndConditionIgnoreUniqueKey(String sql, String injectCondition, Map<String, List<String>> table) {
        return ASTDruidConditionUtil.addCondition(sql, injectCondition, SQLBinaryOperator.BooleanAnd, false,
                ASTDruidConditionUtil.ExistInjectConditionStrategyEnum.RULE_TABLE_MATCH_THEN_SKIP_ITEM, "mysql", null, sqlJoin -> sqlJoin.existUniqueKeyColumn(table));
    }


}
