package com.github.mybatisintercept.util;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;

public class ASTDruidUpdateSetUtil {

    public static String addUpdateSetItem(String sql, String columnName, Object value,
                                          String dbType, BiPredicate<String, String> skip) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        if (stmtList.size() != 1) {
            throw new IllegalArgumentException("not support statement :" + sql);
        }
        SQLStatement ast = stmtList.get(0);
        if (!(ast instanceof SQLUpdateStatement)) {
            return sql;
        }
        boolean change = addUpdateSetItem((SQLUpdateStatement) ast, columnName, value, skip);
        if (change) {
            return SQLUtils.toSQLString(ast, dbType);
        } else {
            return sql;
        }
    }

    private static boolean addUpdateSetItem(SQLUpdateStatement ast, String columnName, Object value,
                                           BiPredicate<String, String> skip) {
        SQLExpr valueExpr = ASTDruidUtil.toValueExpr(value);

        List<SQLExprTableSource> tableList = new ArrayList<>();
        ast.getTableSource().accept(new SQLASTVisitorAdapter() {
            @Override
            public boolean visit(SQLExprTableSource table) {
                tableList.add(table);
                return true;
            }
        });
        boolean change = false;
        for (SQLExprTableSource table : tableList) {
            String tableSchema = SQLUtils.normalize(table.getSchema(), null);
            String tableName = SQLUtils.normalize(table.getName().getSimpleName(), null);
            if (skip != null && skip.test(tableSchema, tableName)) {
                continue;
            }
            String alias = table.getAlias();
            SQLUpdateSetItem setItem = findSetItem(ast.getItems(), columnName, alias);
            if (setItem != null) {
                if (setItem.getValue() instanceof SQLNullExpr) {
                    setItem.setValue(valueExpr);
                    change = true;
                }
            } else {
                SQLUpdateSetItem newSetItem = new SQLUpdateSetItem();
                String owner = alias == null ? tableName : alias;
                newSetItem.setColumn(new SQLPropertyExpr(owner, columnName));
                newSetItem.setValue(valueExpr);
                ast.getItems().add(newSetItem);
                change = true;
            }
        }
        return change;
    }

    private static SQLUpdateSetItem findSetItem(List<SQLUpdateSetItem> list, String columnName,
                                                String tableAlias) {
        for (SQLUpdateSetItem setItem : list) {
            SQLExpr itemColumn = setItem.getColumn();
            if (itemColumn instanceof SQLPropertyExpr) {
                SQLPropertyExpr itemColumnExpr = (SQLPropertyExpr) itemColumn;
                if (SQLUtils.nameEquals(tableAlias, itemColumnExpr.getOwnernName())
                        && SQLUtils.nameEquals(columnName, itemColumnExpr.getSimpleName())) {
                    return setItem;
                }
            } else if (itemColumn instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr itemColumnExpr = (SQLIdentifierExpr) itemColumn;
                if (SQLUtils.nameEquals(columnName, itemColumnExpr.getSimpleName())) {
                    return setItem;
                }
            }
        }
        return null;
    }


}
