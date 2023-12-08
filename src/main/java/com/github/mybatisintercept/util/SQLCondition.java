package com.github.mybatisintercept.util;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SQLCondition {
    public enum TypeEnum {
        COMMA,
        WHERE,
        JOIN
    }

    private final String sql;
    private TypeEnum type;
    private JoinTypeEnum joinTypeEnum;
    private String fromTableAlias;
    private String fromTableName;
    private String fromTableSchema;
    private List<SQLColumn> columnList;

    public SQLCondition(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public TypeEnum getType() {
        return type;
    }

    public JoinTypeEnum getJoinTypeEnum() {
        return joinTypeEnum;
    }

    public boolean isTypeComma() {
        return this.type == TypeEnum.COMMA;
    }

    public boolean isTypeWhere() {
        return this.type == TypeEnum.WHERE;
    }

    public boolean isTypeJoin() {
        return this.type == TypeEnum.JOIN;
    }

    public String getFromTableAlias() {
        return fromTableAlias;
    }

    public String getFromTableName() {
        return fromTableName;
    }

    public String getFromTableSchema() {
        return fromTableSchema;
    }

    public List<SQLColumn> getColumnList() {
        return columnList;
    }

    void reset(TypeEnum typeEnum, JoinTypeEnum joinTypeEnum, String fromTableAlias, String fromTableSchema, String fromTableName, List<SQLColumn> columnList) {
        this.type = typeEnum;
        this.joinTypeEnum = joinTypeEnum;
        this.fromTableAlias = fromTableAlias;
        this.fromTableName = fromTableName;
        this.fromTableSchema = fromTableSchema;
        this.columnList = columnList;
    }

    public boolean isCanIgnoreInject(Map<String, List<TableUniqueIndex>> tableUniqueKeyColumnMap) {
        if (joinTypeEnum == JoinTypeEnum.RIGHT_OUTER_JOIN) {
            if (isTypeJoin()) {
                // 右连接右表
                // 防止越权
                return false;
            } else {
                // 右连接左表
                return existUniqueKeyIndexColumn(tableUniqueKeyColumnMap.get(getFromTableName()));
            }
        } else if (isTypeWhere()) {
            // 防止越权
            return false;
        } else {
            return existUniqueKeyIndexColumn(tableUniqueKeyColumnMap.get(getFromTableName()));
        }
    }

    public boolean existUniqueKeyColumn(Map<String, List<TableUniqueIndex>> tableUniqueKeyColumnMap) {
        return existUniqueKeyIndexColumn(tableUniqueKeyColumnMap.get(getFromTableName()));
    }

    /**
     * 唯一键 = 任意
     *
     * @param indexList
     * @return
     */
    public boolean existUniqueKeyIndexColumn(List<TableUniqueIndex> indexList) {
        if (indexList == null || indexList.isEmpty()) {
            return false;
        }
        for (TableUniqueIndex tableIndex : indexList) {
            if (existUniqueKeyColumn(tableIndex.getColumnNameList())) {
                return true;
            }
        }
        return false;
    }

    public boolean existUniqueKeyColumn(List<String> uniqueKeyColumnList) {
        if (uniqueKeyColumnList == null || uniqueKeyColumnList.isEmpty() || columnList.size() < uniqueKeyColumnList.size()) {
            return false;
        }
        boolean exist = false;
        for (String uniqueKeyColumn : uniqueKeyColumnList) {
            for (SQLColumn sqlColumn : columnList) {
                // 主键 = 任意
                if (sqlColumn.exist(uniqueKeyColumn)) {
                    exist = true;
                    break;
                }
            }
            if (!exist) {
                return false;
            }
        }
        return true;
    }

    public boolean existParameterizedColumn() {
        for (SQLColumn column : columnList) {
            if (column.existParameterized()) {
                return true;
            }
        }
        return false;
    }

    public int getParameterizedColumnCount() {
        int i = 0;
        for (SQLColumn column : columnList) {
            if (column.existParameterized()) {
                i++;
            }
        }
        return i;
    }

    @Override
    public String toString() {
        String string;
        if (fromTableName == null) {
            string = "";
        } else if (fromTableAlias == null) {
            string = "from " + fromTableName;
        } else {
            string = "from " + fromTableName + " as " + fromTableAlias;
        }
        if (columnList == null || columnList.isEmpty()) {
            return string;
        } else {
            return string + " on " + columnList.stream().map(SQLColumn::toString).collect(Collectors.joining(" and "));
        }
    }

}