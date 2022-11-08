package com.github.mybatisintercept.util;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowStatement;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;

import java.util.List;
import java.util.function.BiPredicate;

public class ASTDruidUtil {

    public static String addAndCondition(String sql, String injectCondition, String dbType) {
        return ASTDruidConditionUtil.addCondition(sql, injectCondition, SQLBinaryOperator.BooleanAnd, false,
                ASTDruidConditionUtil.ExistInjectConditionStrategyEnum.RULE_TABLE_MATCH_THEN_SKIP_SQL, dbType, null);
    }

    public static String addAndCondition(String sql, String injectCondition, ASTDruidConditionUtil.ExistInjectConditionStrategyEnum existInjectConditionStrategyEnum, String dbType, BiPredicate<String, String> skip) {
        return ASTDruidConditionUtil.addCondition(sql, injectCondition, SQLBinaryOperator.BooleanAnd, false, existInjectConditionStrategyEnum, dbType, skip);
    }

    public static SQLExpr toValueExpr(Object value) {
        if (value == null) {
            return new SQLNullExpr();
        } else if (value instanceof String) {
            return new SQLCharExpr((String) value);
        } else {
            return SQLUtils.toSQLExpr(String.valueOf(value));
        }
    }

    private static boolean addSelectItem(SQLSelect query, SQLExpr valueExpr) {
        if (query == null) {
            return true;
        }
        SQLSelectQuery select = query.getQuery();
        if (select instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock queryBlock = ((SQLSelectQueryBlock) select);
            queryBlock.addSelectItem(valueExpr);
            return true;
        } else {
            return false;
        }
    }

    public static String addColumnValues(String rawSql, String columnName, Object value, String dbType) {
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(rawSql, dbType);
        if (sqlStatements.size() != 1) {
            throw new IllegalStateException("addColumnValues sqlStatements.size() != 1. sql = " + rawSql);
        }
        SQLStatement sqlStatement = sqlStatements.get(0);
        SQLIdentifierExpr columnExpr = new SQLIdentifierExpr(columnName);

        SQLExpr valueExpr = toValueExpr(value);
        if (sqlStatement instanceof SQLInsertStatement) {
            SQLInsertStatement statement = ((SQLInsertStatement) sqlStatement);
            int columnIndex = columnIndex(statement.getColumns(), columnName);
            SQLSelect query = statement.getQuery();
            // 用户未填写字段
            if (columnIndex == -1) {
                statement.getColumns().add(columnExpr);
                if (query != null) {
                    // insert into `base_area` (`id`, `name`) select id,name from copy
                    if (!addSelectItem(query, valueExpr)) {
                        throw new IllegalStateException("not support addColumnValues. sql = " + rawSql + ", columnName = " + columnName);
                    }
                } else {
                    // insert into `base_area` (`id`, `name`) values (1, '2')
                    for (SQLInsertStatement.ValuesClause valuesClause : statement.getValuesList()) {
                        valuesClause.addValue(valueExpr);
                    }
                }
            } else {
                // 用户填写了字段
                if (query == null) {
                    for (SQLInsertStatement.ValuesClause valuesClause : statement.getValuesList()) {
                        valuesClause.getValues().set(columnIndex, valueExpr);
                    }
                } else {
                    // 这里表示用户如写明了字段，交给用户处理
                }
            }
            return SQLUtils.toSQLString(sqlStatement, dbType);
        } else if (sqlStatement instanceof SQLReplaceStatement) {
            SQLReplaceStatement statement = ((SQLReplaceStatement) sqlStatement);
            SQLQueryExpr query = statement.getQuery();

            int columnIndex = columnIndex(statement.getColumns(), columnName);
            // 用户未填写字段
            if (columnIndex == -1) {
                statement.getColumns().add(columnExpr);
                if (query != null) {
                    // replace into `base_area` (`id`, `name`) select id,name from copy
                    if (!addSelectItem(query.getSubQuery(), valueExpr)) {
                        throw new IllegalStateException("not support addColumnValues. sql = " + rawSql + ", columnName = " + columnName);
                    }
                } else {
                    // replace into `base_area` (`id`, `name`) values (1, '2')
                    for (SQLInsertStatement.ValuesClause valuesClause : statement.getValuesList()) {
                        valuesClause.addValue(valueExpr);
                    }
                }
            } else {
                // 用户填写了字段
                if (query == null) {
                    for (SQLInsertStatement.ValuesClause valuesClause : statement.getValuesList()) {
                        valuesClause.getValues().set(columnIndex, valueExpr);
                    }
                } else {
                    // 这里表示用户如写明了字段，交给用户处理
                }
            }
            return SQLUtils.toSQLString(sqlStatement, dbType);
        } else {
            throw new IllegalStateException("addColumnValues sqlStatements.no support. sql = " + rawSql);
        }
    }

    public static boolean isSupportWhere(SQLStatement statement) {
        if (statement instanceof SQLInsertStatement || statement instanceof SQLReplaceStatement) {
            // INSERT INTO user_copy (id, name) SELECT id, name FROM user
            boolean[] existSelect = new boolean[1];
            statement.accept(new SQLASTVisitorAdapter() {
                @Override
                public boolean visit(SQLSelectQueryBlock x) {
                    existSelect[0] = true;
                    return false;
                }
            });
            return existSelect[0];
        } else if (statement instanceof MySqlShowStatement || statement instanceof SQLSetStatement) {
            return false;
        } else {
            return true;
        }
    }

    public static boolean isNoSkipUpdate(String rawSql, String dbType, BiPredicate<String, String> skip) {
        List<SQLStatement> statementList;
        try {
            statementList = SQLUtils.parseStatements(rawSql, dbType);
        } catch (Exception e) {
            return false;
        }
        // SingleStatement
        if (statementList.size() != 1) {
            return false;
        }
        SQLStatement sqlStatement = statementList.get(0);
        if (sqlStatement instanceof SQLUpdateStatement) {
            SQLTableSource tableSource = ((SQLUpdateStatement) sqlStatement).getTableSource();
            if (tableSource instanceof SQLExprTableSource) {
                // 单表
                SQLExprTableSource table = (SQLExprTableSource) tableSource;
                return !skip.test(SQLUtils.normalize(table.getSchema(), null), SQLUtils.normalize(table.getName().getSimpleName(), null));
            } else if (tableSource instanceof SQLJoinTableSource) {
                // todo 多表
                return true;
            } else {
                // 其他未知语法
                return false;
            }
        } else {
            // 非update语句
            return false;
        }
    }

    public static boolean isNoSkipInsertOrReplace(String rawSql, String dbType, BiPredicate<String, String> skip) {
        List<SQLStatement> statementList;
        try {
            statementList = SQLUtils.parseStatements(rawSql, dbType);
        } catch (Exception e) {
            return false;
        }
        // SingleStatement
        if (statementList.size() != 1) {
            return false;
        }
        SQLStatement sqlStatement = statementList.get(0);
        if (sqlStatement instanceof SQLInsertStatement) {
            SQLExprTableSource table = ((SQLInsertStatement) sqlStatement).getTableSource();
            return !skip.test(SQLUtils.normalize(table.getSchema(), null), SQLUtils.normalize(table.getName().getSimpleName(), null));
        } else if (sqlStatement instanceof SQLReplaceStatement) {
            SQLExprTableSource table = ((SQLReplaceStatement) sqlStatement).getTableSource();
            return !skip.test(SQLUtils.normalize(table.getSchema(), null), SQLUtils.normalize(table.getName().getSimpleName(), null));
        } else {
            return false;
        }
    }

    public static boolean isSingleStatementAndSupportWhere(String rawSql, String dbType) {
        List<SQLStatement> sqlStatements;
        try {
            sqlStatements = SQLUtils.parseStatements(rawSql, dbType);
        } catch (Exception e) {
            return false;
        }
        // SingleStatement
        if (sqlStatements.size() != 1) {
            return false;
        }
        return isSupportWhere(sqlStatements.get(0));
    }

    public static int getColumnParameterizedIndex(String rawSql, String columnName, String dbType) {
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(rawSql, dbType);
        // SingleStatement
        if (sqlStatements.size() != 1) {
            throw new IllegalStateException("getColumns sqlStatements.size() != 1. sql = " + rawSql);
        }
        SQLStatement sqlStatement = sqlStatements.get(0);
        if (sqlStatement instanceof SQLInsertStatement) {
            SQLInsertStatement statement = ((SQLInsertStatement) sqlStatement);
            return columnParameterizedIndex(statement.getColumns(), statement.getValuesList(), columnName);
        } else if (sqlStatement instanceof SQLReplaceStatement) {
            SQLReplaceStatement statement = ((SQLReplaceStatement) sqlStatement);
            return columnParameterizedIndex(statement.getColumns(), statement.getValuesList(), columnName);
        } else {
            throw new IllegalStateException("getColumns not support. sql = " + rawSql);
        }
    }

    private static SQLExpr valueAt(List<SQLExpr> values, int index) {
        if (values == null || values.isEmpty()) {
            return null;
        } else {
            return values.get(index);
        }
    }

    private static int columnParameterizedIndex(List<SQLExpr> columns, List<SQLInsertStatement.ValuesClause> valuesList, String columnName) {
        int i = 0;
        List<SQLExpr> values = valuesList == null || valuesList.isEmpty() ? null : valuesList.get(0).getValues();
        for (SQLExpr column : columns) {
            String name = SQLUtils.normalize(column.toString(), null);
            SQLExpr value = valueAt(values, i);
            if (columnName.equalsIgnoreCase(name)) {
                if (value instanceof SQLVariantRefExpr) {
                    return ((SQLVariantRefExpr) value).getIndex();
                } else {
                    return -2;
                }
            }
            i++;
        }
        return -1;
    }

    private static int columnIndex(List<SQLExpr> columns, String columnName) {
        int i = 0;
        for (SQLExpr column : columns) {
            String name = SQLUtils.normalize(column.toString(), null);
            if (columnName.equalsIgnoreCase(name)) {
                return i;
            }
            i++;
        }
        return -1;
    }

}
