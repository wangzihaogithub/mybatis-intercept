package com.github.mybatisintercept.util;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowStatement;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;

import java.util.LinkedList;
import java.util.List;
import java.util.function.BiPredicate;

public class ASTDruidConditionUtil {

    public static String addCondition(String sql, String injectCondition, SQLBinaryOperator op, boolean left, String dbType, BiPredicate<String, String> skip) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        if (stmtList.size() != 1) {
            throw new IllegalArgumentException("not support statement :" + sql);
        }
        SQLStatement ast = stmtList.get(0);
        boolean change = addCondition(ast, op, SQLUtils.toMySqlExpr(injectCondition), left, wrapDialectSkip(dbType, skip));
        if (change) {
            return SQLUtils.toSQLString(ast, dbType);
        } else {
            return sql;
        }
    }

    public static BiPredicate<String, String> wrapDialectSkip(String dbType, BiPredicate<String, String> skip) {
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
        if (tableSource instanceof SQLExprTableSource) {
            SQLName name = ((SQLExprTableSource) tableSource).getName();
            return name != null ? SQLUtils.normalize(name.getSimpleName(), null) : null;
        } else {
            return null;
        }
    }

    public static String getTableSchema(SQLTableSource tableSource) {
        if (tableSource instanceof SQLExprTableSource) {
            return SQLUtils.normalize(((SQLExprTableSource) tableSource).getSchema(), null);
        } else {
            return null;
        }
    }

    public static boolean addCondition(SQLStatement ast, SQLBinaryOperator op, SQLExpr injectCondition, boolean left, BiPredicate<String, String> skip) {
        if (ast instanceof MySqlShowStatement || ast instanceof SQLSetStatement) {
            return false;
        }
        boolean[] change = new boolean[1];
        ast.accept(new SQLASTVisitorAdapter() {
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
                statement.setWhere(buildCondition(op, injectCondition, alias, left, statement.getWhere()));
                change[0] = true;
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
                statement.setCondition(buildCondition(op, injectCondition, alias, left, statement.getCondition()));
                change[0] = true;
                return true;
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
                        String alias = getAlias(tableSource);
                        if (skip.test(getTableSchema(tableSource), getTableName(tableSource))) {
                            continue;
                        }
                        statement.setWhere(buildCondition(op, injectCondition, alias, left, statement.getWhere()));
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
                        if (skip.test(getTableSchema(tableSource), getTableName(tableSource))) {
                            continue;
                        }
                        statement.setWhere(buildCondition(op, injectCondition, alias, left, statement.getWhere()));
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

    public static SQLExpr buildCondition(SQLBinaryOperator op, SQLExpr injectCondition, String alias, boolean left, SQLExpr where) {
        SQLExpr injectConditionAlias = alias == null ? injectCondition : newConditionIfExistAlias(injectCondition, alias);
        if (where == null) {
            return injectConditionAlias;
        }
        if (left) {
            return new SQLBinaryOpExpr(injectConditionAlias, op, where);
        } else {
            return new SQLBinaryOpExpr(where, op, injectConditionAlias);
        }
    }

    private static SQLExpr newConditionIfExistAlias(SQLExpr condition, String conditionAlias) {
        if (condition instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr expr = (SQLBinaryOpExpr) condition;
            SQLExpr left = expr.getLeft();
            SQLExpr right = expr.getRight();
            SQLExpr newLeft;
            SQLExpr newRight;
            if (left instanceof SQLName) {
                String simpleName = ((SQLName) left).getSimpleName();
                newLeft = new SQLPropertyExpr(conditionAlias, simpleName);
            } else {
                newLeft = left.clone();
            }
            if (right instanceof SQLName) {
                String simpleName = ((SQLName) right).getSimpleName();
                newRight = new SQLPropertyExpr(conditionAlias, simpleName);
            } else {
                newRight = right.clone();
            }
            return new SQLBinaryOpExpr(newLeft, expr.getOperator(), newRight);
        } else {
            throw new IllegalArgumentException("no support condition = " + condition);
        }
    }
}
