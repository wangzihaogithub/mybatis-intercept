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

    public static List<String> getColumnList(String injectCondition) {
        SQLExpr injectConditionExpr = SQLUtils.toSQLExpr(injectCondition, getDbType(null));
        List<String> list = new ArrayList<>();
        injectConditionExpr.accept(new SQLASTVisitorAdapter() {
            boolean select;

            @Override
            public boolean visit(SQLSelectQueryBlock statement) {
                select = true;
                return true;
            }

            @Override
            public void endVisit(SQLSelectQueryBlock x) {
                select = false;
            }

            @Override
            public boolean visit(SQLPropertyExpr x) {
                if (!select) {
                    String col = normalize(x.getName());
                    list.add(col);
                }
                return true;
            }

            @Override
            public boolean visit(SQLIdentifierExpr x) {
                if (!select) {
                    String col = normalize(x.getName());
                    list.add(col);
                }
                return true;
            }
        });
        return list;
    }

    public static String addCondition(String sql, String injectCondition, SQLBinaryOperator op,
                                      boolean appendConditionToLeft, ExistInjectConditionStrategyEnum existInjectConditionStrategyEnum,
                                      String dbType, BiPredicate<String, String> skip, Predicate<SQLCondition> isJoinUniqueKey) {
        return addCondition(sql, injectCondition, op, appendConditionToLeft, existInjectConditionStrategyEnum, dbType, skip, isJoinUniqueKey, null);
    }

    public static String addCondition(String sql, String injectCondition, SQLBinaryOperator op,
                                      boolean appendConditionToLeft, ExistInjectConditionStrategyEnum existInjectConditionStrategyEnum,
                                      String dbType, BiPredicate<String, String> skip, Predicate<SQLCondition> isJoinUniqueKey, List<String> excludeInjectCondition) {
        if (injectCondition == null || injectCondition.isEmpty()) {
            return sql;
        }
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        if (stmtList.size() != 1) {
            throw new IllegalArgumentException("not support statement :" + sql);
        }
        SQLStatement ast = stmtList.get(0);

        SQLExpr injectConditionExpr = SQLUtils.toSQLExpr(injectCondition, getDbType(dbType));
        List<SQLExpr> excludeInjectConditionExprList = getExcludeInjectConditionExprList(excludeInjectCondition, dbType);
        boolean change = addCondition(sql, ast, op, injectConditionExpr, appendConditionToLeft, existInjectConditionStrategyEnum, wrapDialectSkip(dbType, skip), isJoinUniqueKey, excludeInjectConditionExprList);
        if (change) {
            return SQLUtils.toSQLString(ast, dbType);
        } else {
            return sql;
        }
    }

    private static List<SQLExpr> getExcludeInjectConditionExprList(List<String> excludeInjectCondition, String dbType) {
        if (excludeInjectCondition == null) {
            return null;
        }
        List<SQLExpr> list = new ArrayList<>(excludeInjectCondition.size());
        for (String s : excludeInjectCondition) {
            list.add(SQLUtils.toSQLExpr(s, getDbType(dbType)));
        }
        return list;
    }

    private static BiPredicate<String, String> wrapDialectSkip(String dbType, BiPredicate<String, String> skip) {
        switch (dbType) {
            case "MARIADB":
            case "Mariadb":
            case "mariadb":
            case "MYSQL":
            case "Mysql":
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

    private static String getAlias(SQLPropertyExpr expr) {
        if (expr == null) {
            return null;
        } else {
            return normalize(expr.getOwnernName());
        }
    }

    private static String getAlias(SQLTableSource tableSource) {
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

    private static String getTableName(SQLTableSource tableSource) {
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

    private static String getTableSchema(SQLTableSource tableSource) {
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

    private static SQLExpr getCondition(SQLTableSource tableSource) {
        if (tableSource instanceof SQLJoinTableSource) {
            SQLJoinTableSource join = ((SQLJoinTableSource) tableSource);
            SQLTableSource left = join.getLeft();
            if (left instanceof SQLJoinTableSource) {
                return getCondition(left);
            } else {
                return join.getCondition();
            }
        } else {
            return null;
        }
    }

    private static SQLJoinTableSource.JoinType getJoinType(SQLTableSource tableSource) {
        if (tableSource instanceof SQLJoinTableSource) {
            SQLJoinTableSource join = ((SQLJoinTableSource) tableSource);
            SQLTableSource left = join.getLeft();
            if (left instanceof SQLJoinTableSource) {
                return getJoinType(left);
            } else {
                return join.getJoinType();
            }
        } else {
            return null;
        }
    }

    private static boolean existAlias(String alias, SQLExpr condition) {
        if (condition instanceof SQLBinaryOpExpr) {
            LinkedList<SQLBinaryOpExpr> binaryOpExprLinkedList = new LinkedList<>();
            binaryOpExprLinkedList.add((SQLBinaryOpExpr) condition);
            while (!binaryOpExprLinkedList.isEmpty()) {
                SQLBinaryOpExpr binaryOpExpr = binaryOpExprLinkedList.removeFirst();

                SQLExpr left1 = binaryOpExpr.getLeft();
                SQLExpr right1 = binaryOpExpr.getRight();
                if (left1 instanceof SQLBinaryOpExpr) {
                    binaryOpExprLinkedList.add((SQLBinaryOpExpr) left1);
                } else if (left1 instanceof SQLPropertyExpr) {
                    String alias1 = getAlias((SQLPropertyExpr) left1);
                    if (alias.equalsIgnoreCase(alias1)) {
                        return true;
                    }
                }
                if (right1 instanceof SQLBinaryOpExpr) {
                    binaryOpExprLinkedList.add((SQLBinaryOpExpr) right1);
                } else if (left1 instanceof SQLPropertyExpr) {
                    String alias1 = getAlias((SQLPropertyExpr) left1);
                    if (alias.equalsIgnoreCase(alias1)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static boolean existInjectConditionAndPreparedTableAliasMap(SQLStatement readonlyAst,
                                                                        List<SQLName> injectConditionColumnList,
                                                                        BiPredicate<String, String> skip,
                                                                        Map<String, SQLExprTableSource> tableAliasMap) {
        boolean[] exist = new boolean[1];
        readonlyAst.accept(new SQLASTVisitorAdapter() {
            private boolean select;
            private boolean update;
            private boolean delete;

            @Override
            public void endVisit(SQLExprTableSource tableSource) {
                String alias = getAlias(tableSource);
                if (alias != null) {
                    tableAliasMap.put(alias, tableSource);
                }
            }

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

    private static void preparedTableAliasMap(SQLStatement ast, Map<String, SQLExprTableSource> tableAliasMap) {
        ast.accept(new SQLASTVisitorAdapter() {
            @Override
            public void endVisit(SQLExprTableSource tableSource) {
                String alias = getAlias(tableSource);
                if (alias != null) {
                    tableAliasMap.put(alias, tableSource);
                }
            }
        });
    }

    private static boolean addCondition(String sql, SQLStatement ast, SQLBinaryOperator op, SQLExpr injectCondition,
                                        boolean appendConditionToLeft, ExistInjectConditionStrategyEnum existInjectConditionStrategyEnum,
                                        BiPredicate<String, String> skip, Predicate<SQLCondition> isJoinUniqueKey, Collection<SQLExpr> excludeInjectCondition) {
        if (ast instanceof MySqlShowStatement || ast instanceof SQLSetStatement) {
            return false;
        }

        Map<String, SQLExprTableSource> tableAliasMap = new HashMap<>(3);
        List<SQLName> injectConditionColumnList;
        switch (existInjectConditionStrategyEnum) {
            case ANY_TABLE_MATCH_THEN_SKIP_SQL: {
                injectConditionColumnList = flatColumnList(injectCondition);
                if (existInjectConditionAndPreparedTableAliasMap(ast, injectConditionColumnList, (schema, tableName) -> false, tableAliasMap)) {
                    return false;
                }
                break;
            }
            case RULE_TABLE_MATCH_THEN_SKIP_SQL: {
                injectConditionColumnList = flatColumnList(injectCondition);
                if (existInjectConditionAndPreparedTableAliasMap(ast, injectConditionColumnList, skip, tableAliasMap)) {
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

        // 减少1次遍历
        if (tableAliasMap.isEmpty()) {
            preparedTableAliasMap(ast, tableAliasMap);
        }

        injectCondition.accept(InjectMarkSQLASTVisitor.INSTANCE);

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
            public boolean visit(SQLBinaryOpExpr expr) {
                // 1.排除的条件
                return !existExcludeInjectConditionList(excludeInjectCondition, expr);
            }

            @Override
            public boolean visit(SQLSelectQueryBlock statement) {
                select = true;
                return true;
            }

            private boolean addWhere(SQLSelectQueryBlock statement, String alias, String tableSchema, String tableName, SQLCondition.TypeEnum typeEnum, JoinTypeEnum joinTypeEnum, SQLExpr fromCondition) {
                SQLExpr where = statement.getWhere();
                if (isInjectCondition(where)) {
                    return false;
                }
                // 1.规则跳过拼条件
                if (existInjectConditionStrategyEnum == ExistInjectConditionStrategyEnum.RULE_TABLE_MATCH_THEN_SKIP_ITEM
                        && existInjectCondition(injectConditionColumnList, alias, where)) {
                    return false;
                }
                // 2.唯一键跳过拼条件
                if (isJoinUniqueKey != null) {
                    List<SQLColumn> joinUniqueKeyEqualityList = getJoinUniqueKeyEquality(where);
                    if (joinUniqueKeyEqualityList.isEmpty()) {
                        joinUniqueKeyEqualityList = getJoinUniqueKeyEqualityRetry(typeEnum, joinTypeEnum, fromCondition);
                    }
                    if (!joinUniqueKeyEqualityList.isEmpty()) {
                        sqlJoin.reset(typeEnum, joinTypeEnum, alias, tableSchema, tableName, joinUniqueKeyEqualityList);
                        if (sqlJoin.existParameterizedColumn() & isJoinUniqueKey.test(sqlJoin)) {
                            return false;
                        }
                    }
                }
                // 3.拼条件
                statement.setWhere(mergeCondition(op, injectCondition, alias, appendConditionToLeft, where));
                return true;
            }

            private List<SQLColumn> getJoinUniqueKeyEqualityRetry(SQLCondition.TypeEnum typeEnum, JoinTypeEnum joinTypeEnum, SQLExpr fromCondition) {
                if (typeEnum == SQLCondition.TypeEnum.WHERE && joinTypeEnum == JoinTypeEnum.RIGHT_OUTER_JOIN) {
                    return getJoinUniqueKeyEquality(fromCondition);
                } else {
                    return Collections.emptyList();
                }
            }

            @Override
            public void endVisit(SQLSelectQueryBlock statement) {
                if (isInjectCondition(statement)) {
                    return;
                }
                SQLTableSource from = statement.getFrom();
                if (from == null || isSubqueryOrUnion(from)) {
                    return;
                }
                String tableSchema = getTableSchema(from);
                String tableName = getTableName(from);
                if (skip.test(tableSchema, tableName)) {
                    return;
                }
                String alias = getAlias(from);
                JoinTypeEnum joinTypeEnum;
                SQLExpr fromCondition;
                if (from instanceof SQLJoinTableSource) {
                    joinTypeEnum = isLeftJoin(alias, (SQLJoinTableSource) from) ? JoinTypeEnum.LEFT_OUTER_JOIN : JoinTypeEnum.RIGHT_OUTER_JOIN;
                    fromCondition = getCondition(from);
                } else {
                    joinTypeEnum = null;
                    fromCondition = null;
                }
                if (addWhere(statement, alias, tableSchema, tableName, SQLCondition.TypeEnum.WHERE, joinTypeEnum, fromCondition)) {
                    change[0] = true;
                }
                select = false;
            }

            private boolean isLeftJoin(String selectAlias, SQLJoinTableSource from) {
                boolean anyRight = false;
                LinkedList<SQLJoinTableSource> temp = new LinkedList<>();
                temp.add(from);
                while (!temp.isEmpty()) {
                    SQLJoinTableSource join = temp.removeFirst();
                    SQLJoinTableSource.JoinType joinType = join.getJoinType();
                    SQLTableSource left = join.getLeft();
                    SQLTableSource right = join.getRight();

                    if (joinType == SQLJoinTableSource.JoinType.RIGHT_OUTER_JOIN) {
                        anyRight = true;
                    } else {
                        SQLExpr condition = join.getCondition();
                        if (condition == null && joinType == SQLJoinTableSource.JoinType.COMMA) {
                            SQLObject parent = join.getParent();
                            if (parent instanceof SQLSelectQueryBlock) {
                                condition = ((SQLSelectQueryBlock) parent).getWhere();
                            }
                        }
                        if (existAlias(selectAlias, condition)) {
                            return true;
                        }
                    }
                    if (left instanceof SQLJoinTableSource) {
                        temp.add((SQLJoinTableSource) left);
                    }
                    if (right instanceof SQLJoinTableSource) {
                        temp.add((SQLJoinTableSource) right);
                    }
                }
                return !anyRight;
            }

            @Override
            public void endVisit(SQLJoinTableSource statement) {
                if (!isSelect()) {
                    return;
                }
                SQLTableSource from = statement.getRight();
                if (from == null || isSubqueryOrUnion(from)) {
                    return;
                }
                String tableSchema = getTableSchema(from);
                String tableName = getTableName(from);
                if (skip.test(tableSchema, tableName)) {
                    return;
                }
                if (statement.getJoinType() == SQLJoinTableSource.JoinType.COMMA) {
                    // from table1,table2 where table1.id = table2.xx_id
                    SQLObject parent = statement.getParent();
                    if (parent instanceof SQLSelectQueryBlock && addWhere((SQLSelectQueryBlock) parent, getAlias(from), tableSchema, tableName, SQLCondition.TypeEnum.COMMA, JoinTypeEnum.COMMA, null)) {
                        change[0] = true;
                    }
                } else if (addJoin(statement, getAlias(from), tableSchema, tableName)) {
                    // on table1.id = table2.xx_id
                    change[0] = true;
                }
            }

            private boolean addJoin(SQLJoinTableSource join, String alias, String tableSchema, String tableName) {
                SQLExpr condition = join.getCondition();
                if (isInjectCondition(condition)) {
                    return false;
                }

                // 1.规则跳过拼条件
                if (existInjectConditionStrategyEnum == ExistInjectConditionStrategyEnum.RULE_TABLE_MATCH_THEN_SKIP_ITEM
                        && existInjectCondition(injectConditionColumnList, alias, condition)) {
                    return false;
                }
                // 2.唯一键跳过拼条件
                if (isJoinUniqueKey != null) {
                    List<SQLColumn> joinUniqueKeyEqualityList = getJoinUniqueKeyEquality(condition);
                    if (!joinUniqueKeyEqualityList.isEmpty()) {
                        sqlJoin.reset(SQLCondition.TypeEnum.JOIN, codeOfJoinTypeEnum(join.getJoinType()), alias, tableSchema, tableName, joinUniqueKeyEqualityList);
                        if (isJoinUniqueKey.test(sqlJoin)) {
                            return false;
                        }
                    }
                }
                // 3.拼条件
                join.setCondition(mergeCondition(op, injectCondition, alias, appendConditionToLeft, condition));
                return true;
            }

            private JoinTypeEnum codeOfJoinTypeEnum(SQLJoinTableSource.JoinType joinType) {
                if (joinType == null) {
                    return null;
                }
                return JoinTypeEnum.codeOf(joinType.name());
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
                return true;
            }

            @Override
            public void endVisit(SQLDeleteStatement statement) {
                delete = false;

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
                        SQLExpr where = statement.getWhere();
                        if (isInjectCondition(where)) {
                            continue;
                        }
                        if (isSubqueryOrUnion(tableSource) || skip.test(getTableSchema(tableSource), getTableName(tableSource))) {
                            continue;
                        }
                        String alias = getAlias(tableSource);
                        if (existInjectConditionStrategyEnum == ExistInjectConditionStrategyEnum.RULE_TABLE_MATCH_THEN_SKIP_ITEM
                                && existInjectCondition(injectConditionColumnList, alias, where)) {
                            continue;
                        }
                        statement.setWhere(mergeCondition(op, injectCondition, alias, appendConditionToLeft, where));
                        change[0] = true;
                    }
                }
            }

            @Override
            public boolean visit(SQLUpdateStatement statement) {
                update = true;
                return true;
            }

            @Override
            public void endVisit(SQLUpdateStatement statement) {
                update = false;

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
                        SQLExpr where = statement.getWhere();
                        if (isInjectCondition(where)) {
                            continue;
                        }
                        String alias = getAlias(tableSource);
                        if (isSubqueryOrUnion(tableSource) || skip.test(getTableSchema(tableSource), getTableName(tableSource))) {
                            continue;
                        }
                        if (existInjectConditionStrategyEnum == ExistInjectConditionStrategyEnum.RULE_TABLE_MATCH_THEN_SKIP_ITEM
                                && existInjectCondition(injectConditionColumnList, alias, where)) {
                            continue;
                        }
                        statement.setWhere(mergeCondition(op, injectCondition, alias, appendConditionToLeft, where));
                        change[0] = true;
                    }
                }
            }
        });
        return change[0];
    }

    private static List<SQLName> flatColumnList(SQLExpr injectCondition) {
        List<SQLName> list = new ArrayList<>(2);
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

    private static boolean existExcludeInjectConditionList(Collection<SQLExpr> excludeInjectList, SQLExpr where) {
        if (excludeInjectList != null) {
            for (SQLExpr exclude : excludeInjectList) {
                if (existExcludeInjectCondition(exclude, where)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isEquals(SQLObject exclude, SQLObject where) {
        if (exclude == null && where == null) {
            return true;
        }
        if (exclude == null || where == null) {
            return false;
        }
        if (exclude.getClass() != where.getClass()) {
            return false;
        }
        if (exclude instanceof SQLPropertyExpr && where instanceof SQLPropertyExpr) {
            SQLPropertyExpr excludeExpr = (SQLPropertyExpr) exclude;
            SQLPropertyExpr whereExpr = (SQLPropertyExpr) where;
            return equalsIgnoreCase(normalize(excludeExpr.getName()), normalize(whereExpr.getName()));
        } else if (exclude instanceof SQLIdentifierExpr && where instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr excludeExpr = (SQLIdentifierExpr) exclude;
            SQLIdentifierExpr whereExpr = (SQLIdentifierExpr) where;
            return equalsIgnoreCase(normalize(excludeExpr.getName()), normalize(whereExpr.getName()));
        } else if (exclude instanceof SQLBinaryOpExpr && where instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr excludeExpr = (SQLBinaryOpExpr) exclude;
            SQLBinaryOpExpr whereExpr = (SQLBinaryOpExpr) where;
            return excludeExpr.getOperator() == whereExpr.getOperator();
        } else {
            return true;
        }
    }

    public static boolean equalsIgnoreCase(String a, String b) {
        return (a == b) || (a != null && a.equalsIgnoreCase(b));
    }

    private static boolean existExcludeInjectCondition(SQLExpr exclude, SQLExpr where) {
        if (where == null) {
            return false;
        }
        if (!isEquals(exclude, where)) {
            return false;
        }

        LinkedList<SQLObject> tempexclude = new LinkedList<>();
        LinkedList<SQLObject> tempwhere = new LinkedList<>();
        tempexclude.add(exclude);
        tempwhere.add(where);
        while (true) {
            if (tempexclude.size() != tempwhere.size()) {
                return false;
            }
            if (tempexclude.isEmpty()) {
                return true;
            }
            SQLObject excludeExpr = tempexclude.removeFirst();
            SQLObject whereExpr = tempwhere.removeFirst();
            if (!isEquals(excludeExpr, whereExpr)) {
                return false;
            }
            if (excludeExpr instanceof SQLExpr) {
                SQLExpr itemExpr = (SQLExpr) excludeExpr;
                List<SQLObject> next = itemExpr.getChildren();
                if (next != null && !next.isEmpty()) {
                    tempexclude.addAll(next);
                }
            }
            if (whereExpr instanceof SQLExpr) {
                SQLExpr itemExpr = (SQLExpr) whereExpr;
                List<SQLObject> next = itemExpr.getChildren();
                if (next != null && !next.isEmpty()) {
                    tempwhere.addAll(next);
                }
            }
        }
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

    private static SQLExpr mergeCondition(SQLBinaryOperator op, SQLExpr injectCondition, String alias, boolean left, SQLExpr where) {
        if (injectCondition instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr binaryOpExpr = ((SQLBinaryOpExpr) injectCondition);
            SQLExpr injectConditionAlias = alias == null ?
                    injectCondition : mergeConditionIfExistAlias(binaryOpExpr.getLeft(), binaryOpExpr.getRight(), binaryOpExpr.getOperator(), alias);
            if (where == null) {
                return injectConditionAlias;
            }
            return left ? new SQLBinaryOpExpr(injectConditionAlias, op, where) : new SQLBinaryOpExpr(where, op, injectConditionAlias);
        } else {
            return alias == null ?
                    injectCondition : left ? mergeConditionIfExistAlias(injectCondition, where, op, alias) : mergeConditionIfExistAlias(where, injectCondition, op, alias);
        }
    }

    private static SQLExpr mergeConditionIfExistAlias(SQLExpr left, SQLExpr right, SQLBinaryOperator operator, String conditionAlias) {
        SQLExpr newLeft;
        SQLExpr newRight;
        if (left instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr expr = (SQLBinaryOpExpr) left;
            newLeft = mergeConditionIfExistAlias(expr.getLeft(), expr.getRight(), expr.getOperator(), conditionAlias);
        } else if (left instanceof SQLName) {
            String simpleName = ((SQLName) left).getSimpleName();
            newLeft = new SQLPropertyExpr(conditionAlias, simpleName);
        } else if (left instanceof SQLInSubQueryExpr) {
            newLeft = left.clone();
            ((SQLInSubQueryExpr) newLeft).setExpr(mergeConditionIfExistAlias(((SQLInSubQueryExpr) left).getExpr(), null, operator, conditionAlias));
        } else {
            newLeft = left == null ? null : left.clone();
        }

        if (right instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr expr = (SQLBinaryOpExpr) right;
            newRight = mergeConditionIfExistAlias(expr.getLeft(), expr.getRight(), expr.getOperator(), conditionAlias);
        } else if (right instanceof SQLIdentifierExpr) {
            String simpleName = ((SQLName) right).getSimpleName();
            newRight = new SQLPropertyExpr(conditionAlias, simpleName);
        } else if (right instanceof SQLInSubQueryExpr) {
            newRight = right.clone();
            ((SQLInSubQueryExpr) newRight).setExpr(mergeConditionIfExistAlias(((SQLInSubQueryExpr) right).getExpr(), null, operator, conditionAlias));
        } else {
            newRight = right == null ? null : right.clone();
        }
        SQLExpr binaryOpExpr;
        if (newLeft == null) {
            binaryOpExpr = newRight;
        } else if (newRight == null) {
            binaryOpExpr = newLeft;
        } else {
            binaryOpExpr = new SQLBinaryOpExpr(newLeft, operator, newRight);
        }
        if (binaryOpExpr != null) {
            binaryOpExpr.accept(InjectMarkSQLASTVisitor.INSTANCE);
        }
        return binaryOpExpr;
    }

    private static <T> T getDbType(String type) {
        if (DB_TYPE_METHOD != null) {
            try {
                return (T) DB_TYPE_METHOD.invoke(null, type);
            } catch (IllegalAccessException | InvocationTargetException e) {
            }
        }
        return (T) type;
    }

    private static final String INJECT_CONDITION_MARK_NAME = "inject";

    private static boolean isInjectCondition(SQLObject injectCondition) {
        return injectCondition != null && injectCondition.containsAttribute(INJECT_CONDITION_MARK_NAME);
    }

    static class InjectMarkSQLASTVisitor extends SQLASTVisitorAdapter {
        static final InjectMarkSQLASTVisitor INSTANCE = new InjectMarkSQLASTVisitor();

        @Override
        public boolean visit(SQLBinaryOpExpr x) {
            injectMark(x);
            return true;
        }

        @Override
        public boolean visit(SQLSelectQueryBlock x) {
            injectMark(x);
            return true;
        }

        private void injectMark(SQLObject sqlObject) {
            sqlObject.putAttribute(INJECT_CONDITION_MARK_NAME, Boolean.TRUE);
        }
    }

}
