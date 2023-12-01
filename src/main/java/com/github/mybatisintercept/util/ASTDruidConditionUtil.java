package com.github.mybatisintercept.util;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowStatement;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;
import com.alibaba.druid.sql.visitor.SQLEvalVisitor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

public class ASTDruidConditionUtil {
    private static final Method DB_TYPE_METHOD;

    static {
        Method dbTypeMethod;
        try {
            Class<?> clazz = Class.forName("com.alibaba.druid.DbType");
            dbTypeMethod = clazz.getDeclaredMethod("of", String.class);
        } catch (Exception e) {
            dbTypeMethod = null;
        }
        DB_TYPE_METHOD = dbTypeMethod;
    }

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

    public static List<String> getColumnList(String injectCondition) {
        SQLExpr injectConditionExpr = SQLUtils.toSQLExpr(injectCondition, getDbType(null));
        List<String> list = new ArrayList<>();
        injectConditionExpr.accept(new SQLASTVisitorAdapter() {
            @Override
            public boolean visit(SQLPropertyExpr x) {
                String col = normalize(x.getName());
                list.add(col);
                return true;
            }

            @Override
            public boolean visit(SQLIdentifierExpr x) {
                String col = normalize(x.getName());
                list.add(col);
                return true;
            }
        });
        return list;
    }

    public static String addCondition(String sql, String injectCondition, SQLBinaryOperator op,
                                      boolean appendConditionToLeft, ExistInjectConditionStrategyEnum existInjectConditionStrategyEnum,
                                      String dbType, BiPredicate<String, String> skip, Predicate<SQLCondition> isJoinUniqueKey) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        if (stmtList.size() != 1) {
            throw new IllegalArgumentException("not support statement :" + sql);
        }
        SQLStatement ast = stmtList.get(0);

        SQLExpr injectConditionExpr = SQLUtils.toSQLExpr(injectCondition, getDbType(dbType));
        if (!(injectConditionExpr instanceof SQLBinaryOpExpr)) {
            throw new IllegalArgumentException("no support injectCondition = " + injectCondition);
        }
        boolean change = addCondition(sql, ast, op, (SQLBinaryOpExpr) injectConditionExpr, appendConditionToLeft, existInjectConditionStrategyEnum, wrapDialectSkip(dbType, skip), isJoinUniqueKey);
        if (change) {
            return SQLUtils.toSQLString(ast, dbType);
        } else {
            return sql;
        }
    }

    private static BiPredicate<String, String> wrapDialectSkip(String dbType, BiPredicate<String, String> skip) {
        switch (dbType) {
            case "MARIADB":
            case "mariadb":
            case "MYSQL":
            case "mysql": {
                return (schema, tableName) -> {
                    if ("dual".equalsIgnoreCase(tableName)) {
                        return true;
                    } else {
                        return skip != null && skip.test(schema, tableName);
                    }
                };
            }
            default: {
                return skip == null ? (schema, tableName) -> false : skip;
            }
        }
    }

    public static String getAlias(SQLTableSource tableSource) {
        if (tableSource == null) {
            // 这种sql => SELECT @rownum := 0, @rowtotal := NULL
            return null;
        } else if (tableSource instanceof SQLJoinTableSource) {
            // join
            return getAlias(((SQLJoinTableSource) tableSource).getLeft());
        } else {
            String alias = tableSource.getAlias();
            if (alias != null) {
                return alias;
            } else {
                return getTableName(tableSource);
            }
        }
    }

    private static boolean isSubqueryOrUnion(SQLTableSource from) {
        if (from instanceof SQLJoinTableSource) {
            return isSubqueryOrUnion(((SQLJoinTableSource) from).getLeft());
        } else if (from instanceof SQLSubqueryTableSource) {
            // 子查询
            return true;
        } else if (from instanceof SQLUnionQueryTableSource) {
            // 联合查询
            return true;
        } else if (from instanceof SQLWithSubqueryClause) {
            // mysql 8 With
            return true;
        } else {
            return false;
        }
    }

    public static String getTableName(SQLTableSource tableSource) {
        if (tableSource == null) {
            // 这种sql => SELECT @rownum := 0, @rowtotal := NULL
            return null;
        } else if (tableSource instanceof SQLJoinTableSource) {
            // join
            return getTableName(((SQLJoinTableSource) tableSource).getLeft());
        } else if (tableSource instanceof SQLExprTableSource) {
            SQLName name = ((SQLExprTableSource) tableSource).getName();
            return name != null ? normalize(name.getSimpleName()) : null;
        } else {
            return null;
        }
    }

    public static String getTableSchema(SQLTableSource tableSource) {
        if (tableSource == null) {
            // 这种sql => SELECT @rownum := 0, @rowtotal := NULL
            return null;
        } else if (tableSource instanceof SQLJoinTableSource) {
            // join
            return getTableSchema(((SQLJoinTableSource) tableSource).getLeft());
        } else if (tableSource instanceof SQLExprTableSource) {
            return normalize(((SQLExprTableSource) tableSource).getSchema());
        } else {
            return null;
        }
    }

    private static String normalize(String name) {
        return SQLUtils.normalize(name, null);
    }

    private static boolean existInjectCondition(SQLStatement readonlyAst,
                                                List<SQLName> injectConditionColumnList,
                                                BiPredicate<String, String> skip) {
        boolean[] exist = new boolean[1];
        readonlyAst.accept(new SQLASTVisitorAdapter() {
            private boolean select;
            private boolean update;
            private boolean delete;

            @Override
            public boolean visit(SQLSelectQueryBlock statement) {
                select = true;
                SQLTableSource from = statement.getFrom();
                if (from == null || isSubqueryOrUnion(from) || skip.test(getTableSchema(from), getTableName(from))) {
                    return true;
                }
                String alias = getAlias(from);
                if (existInjectCondition(injectConditionColumnList, alias, statement.getWhere())) {
                    exist[0] = true;
                    return false;
                }
                return true;
            }

            @Override
            public void endVisit(SQLSelectQueryBlock x) {
                select = false;
            }

            @Override
            public boolean visit(SQLJoinTableSource statement) {
                if (!select) {
                    return true;
                }
                SQLTableSource from = statement.getRight();
                if (from == null || isSubqueryOrUnion(from) || skip.test(getTableSchema(from), getTableName(from))) {
                    return true;
                }
                String alias = getAlias(from);
                switch (statement.getJoinType()) {
                    case COMMA: {
                        SQLObject parent = statement.getParent();
                        if (parent instanceof SQLSelectQueryBlock && existInjectCondition(injectConditionColumnList, alias, ((SQLSelectQueryBlock) parent).getWhere())) {
                            exist[0] = true;
                            return false;
                        }
                        return true;
                    }
                    default: {
                        if (existInjectCondition(injectConditionColumnList, alias, statement.getCondition())) {
                            exist[0] = true;
                            return false;
                        }
                        return true;
                    }
                }
            }

            @Override
            public boolean visit(SQLDeleteStatement statement) {
                delete = true;

                LinkedList<SQLTableSource> temp = new LinkedList<>();
                temp.add(statement.getTableSource());
                while (!temp.isEmpty()) {
                    SQLTableSource tableSource = temp.removeFirst();
                    if (tableSource == null) {
                        continue;
                    }

                    if (tableSource instanceof SQLJoinTableSource) {
                        temp.add(((SQLJoinTableSource) tableSource).getLeft());
                        temp.add(((SQLJoinTableSource) tableSource).getRight());
                    } else {
                        if (isSubqueryOrUnion(tableSource) || skip.test(getTableSchema(tableSource), getTableName(tableSource))) {
                            continue;
                        }
                        String alias = getAlias(tableSource);
                        if (existInjectCondition(injectConditionColumnList, alias, statement.getWhere())) {
                            exist[0] = true;
                            return false;
                        }
                    }
                }
                return true;
            }

            @Override
            public void endVisit(SQLDeleteStatement x) {
                delete = false;
            }

            @Override
            public boolean visit(SQLUpdateStatement statement) {
                update = true;

                LinkedList<SQLTableSource> temp = new LinkedList<>();
                temp.add(statement.getTableSource());
                while (!temp.isEmpty()) {
                    SQLTableSource tableSource = temp.removeFirst();
                    if (tableSource == null) {
                        continue;
                    }

                    if (tableSource instanceof SQLJoinTableSource) {
                        temp.add(((SQLJoinTableSource) tableSource).getLeft());
                        temp.add(((SQLJoinTableSource) tableSource).getRight());
                    } else {
                        String alias = getAlias(tableSource);
                        if (isSubqueryOrUnion(tableSource) || skip.test(getTableSchema(tableSource), getTableName(tableSource))) {
                            continue;
                        }
                        if (existInjectCondition(injectConditionColumnList, alias, statement.getWhere())) {
                            exist[0] = true;
                            return false;
                        }
                    }
                }
                return true;
            }

            @Override
            public void endVisit(SQLUpdateStatement x) {
                update = false;
            }
        });
        return exist[0];
    }

    private static boolean addCondition(String sql, SQLStatement ast, SQLBinaryOperator op, SQLBinaryOpExpr injectCondition,
                                        boolean appendConditionToLeft, ExistInjectConditionStrategyEnum existInjectConditionStrategyEnum,
                                        BiPredicate<String, String> skip, Predicate<SQLCondition> isJoinUniqueKey) {
        if (ast instanceof MySqlShowStatement || ast instanceof SQLSetStatement) {
            return false;
        }
        List<SQLName> injectConditionColumnList;
        switch (existInjectConditionStrategyEnum) {
            case ANY_TABLE_MATCH_THEN_SKIP_SQL: {
                injectConditionColumnList = flatColumnList(injectCondition);
                if (existInjectCondition(ast, injectConditionColumnList, (schema, tableName) -> false)) {
                    return false;
                }
                break;
            }
            case RULE_TABLE_MATCH_THEN_SKIP_SQL: {
                injectConditionColumnList = flatColumnList(injectCondition);
                if (existInjectCondition(ast, injectConditionColumnList, skip)) {
                    return false;
                }
                break;
            }
            case RULE_TABLE_MATCH_THEN_SKIP_ITEM: {
                injectConditionColumnList = flatColumnList(injectCondition);
                break;
            }
            default:
            case ALWAYS_APPEND: {
                injectConditionColumnList = Collections.emptyList();
                break;
            }
        }
        Map<String, SQLExprTableSource> tableAliasMap = new HashMap<>(3);
        ast.accept(new SQLASTVisitorAdapter() {
            @Override
            public boolean visit(SQLExprTableSource tableSource) {
                String alias = getAlias(tableSource);
                if (alias != null) {
                    tableAliasMap.put(alias, tableSource);
                }
                return true;
            }
        });

        boolean[] change = new boolean[1];
        ast.accept(new SQLASTVisitorAdapter() {
            private final SQLCondition sqlJoin = new SQLCondition(sql);
            private final SQLColumn sqlColumn = new SQLColumn();
            private boolean select;
            private boolean update;
            private boolean delete;

            private boolean isSelect() {
                if (select) {
                    return true;
                }
                return !update && !delete;
            }

            @Override
            public boolean visit(SQLSelectQueryBlock statement) {
                select = true;
                SQLTableSource from = statement.getFrom();
                if (from == null || isSubqueryOrUnion(from)) {
                    return true;
                }
                String tableSchema = getTableSchema(from);
                String tableName = getTableName(from);
                if (skip.test(tableSchema, tableName)) {
                    return true;
                }
                if (addWhere(statement, getAlias(from), tableSchema, tableName, SQLCondition.TypeEnum.WHERE)) {
                    change[0] = true;
                }
                return true;
            }

            private boolean addWhere(SQLSelectQueryBlock statement, String alias, String tableSchema, String tableName, SQLCondition.TypeEnum typeEnum) {
                SQLExpr where = statement.getWhere();
                // 1.规则跳过拼条件
                if (existInjectConditionStrategyEnum == ExistInjectConditionStrategyEnum.RULE_TABLE_MATCH_THEN_SKIP_ITEM
                        && existInjectCondition(injectConditionColumnList, alias, where)) {
                    return false;
                }
                // 2.唯一键跳过拼条件
                if (isJoinUniqueKey != null) {
                    List<SQLColumn> joinUniqueKeyEqualityList = getJoinUniqueKeyEquality(where);
                    if (!joinUniqueKeyEqualityList.isEmpty()) {
                        sqlJoin.reset(typeEnum, alias, tableSchema, tableName, joinUniqueKeyEqualityList);
                        int parameterizedColumnCount = sqlJoin.getParameterizedColumnCount();
                        if (parameterizedColumnCount > 0 & isJoinUniqueKey.test(sqlJoin)) {
                            return false;
                        }
                    }
                }
                // 3.拼条件
                statement.setWhere(mergeCondition(op, injectCondition, alias, appendConditionToLeft, where));
                return true;
            }

            @Override
            public void endVisit(SQLSelectQueryBlock x) {
                select = false;
            }

            @Override
            public boolean visit(SQLJoinTableSource statement) {
                if (!isSelect()) {
                    return true;
                }
                SQLTableSource from = statement.getRight();
                if (from == null || isSubqueryOrUnion(from)) {
                    return true;
                }
                String tableSchema = getTableSchema(from);
                String tableName = getTableName(from);
                if (skip.test(tableSchema, tableName)) {
                    return true;
                }
                if (statement.getJoinType() == SQLJoinTableSource.JoinType.COMMA) {
                    // from table1,table2 where table1.id = table2.xx_id
                    SQLObject parent = statement.getParent();
                    if (parent instanceof SQLSelectQueryBlock && addWhere((SQLSelectQueryBlock) parent, getAlias(from), tableSchema, tableName, SQLCondition.TypeEnum.COMMA)) {
                        change[0] = true;
                    }
                } else if (addJoin(statement, getAlias(from), tableSchema, tableName)) {
                    // on table1.id = table2.xx_id
                    change[0] = true;
                }
                return true;
            }

            private boolean addJoin(SQLJoinTableSource join, String alias, String tableSchema, String tableName) {
                SQLExpr condition = join.getCondition();
                // 1.规则跳过拼条件
                if (existInjectConditionStrategyEnum == ExistInjectConditionStrategyEnum.RULE_TABLE_MATCH_THEN_SKIP_ITEM
                        && existInjectCondition(injectConditionColumnList, alias, condition)) {
                    return false;
                }
                // 2.唯一键跳过拼条件
                if (isJoinUniqueKey != null) {
                    List<SQLColumn> joinUniqueKeyEqualityList = getJoinUniqueKeyEquality(condition);
                    if (!joinUniqueKeyEqualityList.isEmpty()) {
                        sqlJoin.reset(SQLCondition.TypeEnum.JOIN, alias, tableSchema, tableName, joinUniqueKeyEqualityList);
                        if (isJoinUniqueKey.test(sqlJoin)) {
                            return false;
                        }
                    }
                }
                // 3.拼条件
                join.setCondition(mergeCondition(op, injectCondition, alias, appendConditionToLeft, condition));
                return true;
            }

            private List<SQLColumn> getJoinUniqueKeyEquality(SQLExpr condition) {
                if (!(condition instanceof SQLBinaryOpExpr)) {
                    return Collections.emptyList();
                }
                SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) condition;
                SQLBinaryOperator operator = binaryOpExpr.getOperator();
                if (operator == SQLBinaryOperator.BooleanAnd) {
                    List<SQLColumn> leftColumn = getJoinUniqueKeyEquality(binaryOpExpr.getLeft());
                    List<SQLColumn> rightColumn = getJoinUniqueKeyEquality(binaryOpExpr.getRight());
                    List<SQLColumn> list = new ArrayList<>(leftColumn.size() + rightColumn.size());
                    list.addAll(leftColumn);
                    list.addAll(rightColumn);
                    return list;
                }
                if (operator != SQLBinaryOperator.Equality) {
                    return Collections.emptyList();
                }
                SQLExpr left = binaryOpExpr.getLeft();
                SQLExpr right = binaryOpExpr.getRight();

                if (left instanceof SQLPropertyExpr) {
                    SQLPropertyExpr itemColumn = (SQLPropertyExpr) left;
                    String columnOwnerName = normalize(itemColumn.getOwnernName());
                    String columnName = normalize(itemColumn.getName());
                    SQLExprTableSource tableSource = getTableSource(columnOwnerName);
                    if (tableSource != null && columnName != null) {
                        String tableName = getTableName(tableSource);
                        String tableSchema = getTableSchema(tableSource);
                        sqlColumn.resetLeftColumn(null, columnOwnerName, tableSchema, tableName, columnName);
                    } else {
                        sqlColumn.resetLeftColumn(null, columnOwnerName, null, null, columnName);
                    }
                } else if (left instanceof SQLValuableExpr) {
                    sqlColumn.resetLeftColumn(value(((SQLValuableExpr) left).getValue()), null, null, null, null);
                    return Collections.singletonList(sqlColumn.clone());
                } else if (left instanceof SQLVariantRefExpr) {
                    sqlColumn.resetLeftColumn(value(left), null, null, null, null);
                    return Collections.singletonList(sqlColumn.clone());
                } else if (left instanceof SQLIdentifierExpr) {
                    SQLIdentifierExpr itemColumn = (SQLIdentifierExpr) left;
                    String columnName = normalize(itemColumn.getName());
                    SQLExprTableSource tableSource = getTableSource(null);
                    if (tableSource != null && columnName != null) {
                        String tableName = getTableName(tableSource);
                        String tableSchema = getTableSchema(tableSource);
                        sqlColumn.resetLeftColumn(null, null, tableSchema, tableName, columnName);
                    } else {
                        sqlColumn.resetLeftColumn(null, null, null, null, columnName);
                    }
                } else {
                    return Collections.emptyList();
                }

                if (right instanceof SQLPropertyExpr) {
                    SQLPropertyExpr itemColumn = (SQLPropertyExpr) right;
                    String columnOwnerName = normalize(itemColumn.getOwnernName());
                    String columnName = normalize(itemColumn.getName());
                    SQLExprTableSource tableSource = tableAliasMap.get(columnOwnerName);
                    if (tableSource != null && columnName != null) {
                        String tableName = getTableName(tableSource);
                        String tableSchema = getTableSchema(tableSource);
                        sqlColumn.resetRightColumn(null, columnOwnerName, tableSchema, tableName, columnName);
                        return Collections.singletonList(sqlColumn.clone());
                    } else {
                        return Collections.emptyList();
                    }
                } else if (right instanceof SQLValuableExpr) {
                    sqlColumn.resetRightColumn(value(((SQLValuableExpr) right).getValue()), null, null, null, null);
                    return Collections.singletonList(sqlColumn.clone());
                } else if (right instanceof SQLVariantRefExpr) {
                    sqlColumn.resetRightColumn(value(right), null, null, null, null);
                    return Collections.singletonList(sqlColumn.clone());
                } else if (right instanceof SQLIdentifierExpr) {
                    SQLIdentifierExpr itemColumn = (SQLIdentifierExpr) right;
                    String columnName = normalize(itemColumn.getName());
                    SQLExprTableSource tableSource = getTableSource(null);
                    if (tableSource != null && columnName != null) {
                        String tableName = getTableName(tableSource);
                        String tableSchema = getTableSchema(tableSource);
                        sqlColumn.resetLeftColumn(null, null, tableSchema, tableName, columnName);
                    } else {
                        sqlColumn.resetLeftColumn(null, null, null, null, columnName);
                    }
                    return Collections.singletonList(sqlColumn.clone());
                } else {
                    return Collections.emptyList();
                }
            }

            private Object value(Object value) {
                if (value == SQLEvalVisitor.EVAL_VALUE_NULL) {
                    return SQLColumn.NULL;
                } else if (value instanceof SQLVariantRefExpr) {
                    return SQLColumn.VAR_REF;
                } else {
                    return value;
                }
            }

            private SQLExprTableSource getTableSource(String alias) {
                SQLExprTableSource tableSource = alias != null ? tableAliasMap.get(alias) : null;
                if (tableSource == null && tableAliasMap.size() == 1) {
                    tableSource = tableAliasMap.values().iterator().next();
                }
                return tableSource;
            }

            @Override
            public boolean visit(SQLDeleteStatement statement) {
                delete = true;

                LinkedList<SQLTableSource> temp = new LinkedList<>();
                temp.add(statement.getTableSource());
                while (!temp.isEmpty()) {
                    SQLTableSource tableSource = temp.removeFirst();
                    if (tableSource == null) {
                        continue;
                    }

                    if (tableSource instanceof SQLJoinTableSource) {
                        temp.add(((SQLJoinTableSource) tableSource).getLeft());
                        temp.add(((SQLJoinTableSource) tableSource).getRight());
                    } else {
                        if (isSubqueryOrUnion(tableSource) || skip.test(getTableSchema(tableSource), getTableName(tableSource))) {
                            continue;
                        }
                        String alias = getAlias(tableSource);
                        if (existInjectConditionStrategyEnum == ExistInjectConditionStrategyEnum.RULE_TABLE_MATCH_THEN_SKIP_ITEM
                                && existInjectCondition(injectConditionColumnList, alias, statement.getWhere())) {
                            continue;
                        }
                        statement.setWhere(mergeCondition(op, injectCondition, alias, appendConditionToLeft, statement.getWhere()));
                        change[0] = true;
                    }
                }
                return true;
            }

            @Override
            public void endVisit(SQLDeleteStatement x) {
                delete = false;
            }

            @Override
            public boolean visit(SQLUpdateStatement statement) {
                update = true;

                LinkedList<SQLTableSource> temp = new LinkedList<>();
                temp.add(statement.getTableSource());
                while (!temp.isEmpty()) {
                    SQLTableSource tableSource = temp.removeFirst();
                    if (tableSource == null) {
                        continue;
                    }

                    if (tableSource instanceof SQLJoinTableSource) {
                        temp.add(((SQLJoinTableSource) tableSource).getLeft());
                        temp.add(((SQLJoinTableSource) tableSource).getRight());
                    } else {
                        String alias = getAlias(tableSource);
                        if (isSubqueryOrUnion(tableSource) || skip.test(getTableSchema(tableSource), getTableName(tableSource))) {
                            continue;
                        }
                        if (existInjectConditionStrategyEnum == ExistInjectConditionStrategyEnum.RULE_TABLE_MATCH_THEN_SKIP_ITEM
                                && existInjectCondition(injectConditionColumnList, alias, statement.getWhere())) {
                            continue;
                        }
                        statement.setWhere(mergeCondition(op, injectCondition, alias, appendConditionToLeft, statement.getWhere()));
                        change[0] = true;
                    }
                }
                return true;
            }

            @Override
            public void endVisit(SQLUpdateStatement x) {
                update = false;
            }
        });
        return change[0];
    }

    private static List<SQLName> flatColumnList(SQLExpr injectCondition) {
        List<SQLName> list = new ArrayList<>();
        LinkedList<SQLObject> temp = new LinkedList<>();
        temp.add(injectCondition);
        while (!temp.isEmpty()) {
            SQLObject injectConditionItem = temp.removeFirst();
            if (injectConditionItem instanceof SQLName) {
                list.add((SQLName) injectConditionItem);
            }
            if (injectConditionItem instanceof SQLExpr) {
                SQLExpr itemExpr = (SQLExpr) injectConditionItem;
                List<SQLObject> next = itemExpr.getChildren();
                if (next != null && !next.isEmpty()) {
                    temp.addAll(next);
                }
            }
        }
        return list;
    }

    private static boolean existInjectCondition(List<SQLName> injectConditionColumnList, String aliasOrTableName, SQLExpr where) {
        if (where == null) {
            return false;
        }
        int injectConditionColumnSize = injectConditionColumnList.size();
        BitSet exist = new BitSet(injectConditionColumnSize);
        LinkedList<SQLObject> temp = new LinkedList<>();
        temp.add(where);
        while (!temp.isEmpty()) {
            SQLObject item = temp.removeFirst();
            if (item == null) {
                continue;
            }

            if (item instanceof SQLExpr) {
                SQLExpr itemExpr = (SQLExpr) item;
                String itemColumnName;
                String itemColumnOwnerName;
                if (item instanceof SQLPropertyExpr) {
                    SQLPropertyExpr itemColumn = (SQLPropertyExpr) item;
                    itemColumnOwnerName = itemColumn.getOwnernName();
                    itemColumnName = itemColumn.getName();
                } else if (item instanceof SQLIdentifierExpr) {
                    itemColumnName = ((SQLIdentifierExpr) item).getName();
                    itemColumnOwnerName = null;
                } else {
                    itemColumnName = null;
                    itemColumnOwnerName = null;
                }

                if (itemColumnOwnerName != null && !itemColumnOwnerName.equalsIgnoreCase(aliasOrTableName)) {
                    continue;
                }
                if (itemColumnName != null) {
                    String normalizeItemColumnName = normalize(itemColumnName);
                    int i = 0;
                    for (SQLName injectConditionColumn : injectConditionColumnList) {
                        if (exist.get(i)) {
                            continue;
                        }
                        if (injectConditionColumn.getSimpleName().equalsIgnoreCase(normalizeItemColumnName)) {
                            exist.set(i);
                        }
                        if (exist.length() == injectConditionColumnSize) {
                            return true;
                        }
                        i++;
                    }
                }

                List<SQLObject> next = itemExpr.getChildren();
                if (next != null && !next.isEmpty()) {
                    temp.addAll(next);
                }
            }
        }
        return exist.length() == injectConditionColumnSize;
    }

    private static SQLExpr mergeCondition(SQLBinaryOperator op, SQLBinaryOpExpr injectCondition, String alias, boolean left, SQLExpr where) {
        SQLExpr injectConditionAlias = alias == null ?
                injectCondition : newConditionIfExistAlias(injectCondition.getLeft(), injectCondition.getRight(), injectCondition.getOperator(), alias);
        if (where == null) {
            return injectConditionAlias;
        }
        if (left) {
            return new SQLBinaryOpExpr(injectConditionAlias, op, where);
        } else {
            return new SQLBinaryOpExpr(where, op, injectConditionAlias);
        }
    }

    private static SQLExpr newConditionIfExistAlias(SQLExpr left, SQLExpr right, SQLBinaryOperator operator, String conditionAlias) {
        SQLExpr newLeft;
        SQLExpr newRight;
        if (left instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr expr = (SQLBinaryOpExpr) left;
            newLeft = newConditionIfExistAlias(expr.getLeft(), expr.getRight(), expr.getOperator(), conditionAlias);
        } else if (left instanceof SQLName) {
            String simpleName = ((SQLName) left).getSimpleName();
            newLeft = new SQLPropertyExpr(conditionAlias, simpleName);
        } else {
            newLeft = left.clone();
        }
        if (right instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr expr = (SQLBinaryOpExpr) right;
            newRight = newConditionIfExistAlias(expr.getLeft(), expr.getRight(), expr.getOperator(), conditionAlias);
        } else if (right instanceof SQLIdentifierExpr) {
            String simpleName = ((SQLName) right).getSimpleName();
            newRight = new SQLPropertyExpr(conditionAlias, simpleName);
        } else {
            newRight = right.clone();
        }
        return new SQLBinaryOpExpr(newLeft, operator, newRight);
    }

    public static <T> T getDbType(String type) {
        if (DB_TYPE_METHOD != null) {
            try {
                return (T) DB_TYPE_METHOD.invoke(null, type);
            } catch (IllegalAccessException | InvocationTargetException e) {
            }
        }
        return (T) type;
    }

}
