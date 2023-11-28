package com.github.mybatisintercept.util;

import java.util.Objects;

public class SQLColumn implements Cloneable {
    public static final Object NULL = new Object() {
        @Override
        public String toString() {
            return "`null`";
        }
    };
    public static final Object VAR_REF = new Object() {
        @Override
        public String toString() {
            return "`?`";
        }
    };
    private String leftTableAlias;
    private String leftTableName;
    private String leftTableSchema;
    private String leftColumnName;
    private Object leftColumnValue;

    private String rightTableAlias;
    private String rightTableName;
    private String rightTableSchema;
    private String rightColumnName;
    private Object rightColumnValue;

    public String getLeftTableAlias() {
        return leftTableAlias;
    }

    public String getLeftTableName() {
        return leftTableName;
    }

    public String getLeftTableSchema() {
        return leftTableSchema;
    }

    public String getLeftColumnName() {
        return leftColumnName;
    }

    public Object getLeftColumnValue() {
        return leftColumnValue;
    }

    public String getRightColumnName() {
        return rightColumnName;
    }

    public String getRightTableAlias() {
        return rightTableAlias;
    }

    public String getRightTableName() {
        return rightTableName;
    }

    public String getRightTableSchema() {
        return rightTableSchema;
    }

    public Object getRightColumnValue() {
        return rightColumnValue;
    }

    void resetLeftColumn(Object columnValue, String tableAlias, String tableSchema, String tableName, String columnName) {
        this.leftColumnValue = columnValue;
        this.leftTableAlias = tableAlias;
        this.leftTableName = tableName;
        this.leftTableSchema = tableSchema;
        this.leftColumnName = columnName;
    }

    void resetRightColumn(Object columnValue, String tableAlias, String tableSchema, String tableName, String columnName) {
        this.rightColumnValue = columnValue;
        this.rightTableAlias = tableAlias;
        this.rightTableName = tableName;
        this.rightTableSchema = tableSchema;
        this.rightColumnName = columnName;
    }

    public boolean exist(String columnName) {
        return Objects.equals(leftColumnName, columnName) || Objects.equals(rightColumnName, columnName);
    }

    public boolean existParameterized() {
        return leftColumnValue != null || rightColumnValue != null;
    }

    @Override
    public String toString() {
        return "(" + leftToString() + " = " + rightToString() + ")";
    }

    public String leftToString() {
        if (leftColumnValue == null) {
            return leftTableName + " as " + leftTableAlias + '.' + leftColumnName;
        } else {
            return String.valueOf(leftColumnValue);
        }
    }

    public String rightToString() {
        if (rightColumnValue == null) {
            return rightTableName + " as " + rightTableAlias + '.' + rightColumnName;
        } else {
            return String.valueOf(rightColumnValue);
        }
    }

    @Override
    public SQLColumn clone() {
        SQLColumn sqlColumn = new SQLColumn();

        sqlColumn.leftTableAlias = leftTableAlias;
        sqlColumn.leftTableName = leftTableName;
        sqlColumn.leftTableSchema = leftTableSchema;
        sqlColumn.leftColumnName = leftColumnName;
        sqlColumn.leftColumnValue = leftColumnValue;

        sqlColumn.rightTableAlias = rightTableAlias;
        sqlColumn.rightTableName = rightTableName;
        sqlColumn.rightTableSchema = rightTableSchema;
        sqlColumn.rightColumnName = rightColumnName;
        sqlColumn.rightColumnValue = rightColumnValue;
        return sqlColumn;
    }
}