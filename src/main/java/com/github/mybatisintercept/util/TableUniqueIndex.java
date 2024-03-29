package com.github.mybatisintercept.util;

import java.util.List;
import java.util.Objects;

public class TableUniqueIndex {
    public static final String PRIMARY_NAME = "PRIMARY";
    private final String indexName;
    private final List<String> columnNameList;

    public TableUniqueIndex(List<String> columnNameList) {
        this.indexName = PRIMARY_NAME;
        this.columnNameList = columnNameList;
    }

    public TableUniqueIndex(String indexName, List<String> columnNameList) {
        this.indexName = indexName;
        this.columnNameList = columnNameList;
    }

    public List<String> getColumnNameList() {
        return columnNameList;
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableUniqueIndex)) {
            return false;
        }
        TableUniqueIndex that = (TableUniqueIndex) o;
        return Objects.equals(indexName, that.indexName) && Objects.equals(columnNameList, that.columnNameList);
    }

    @Override
    public int hashCode() {
        int result = indexName != null ? indexName.hashCode() : 0;
        result = 31 * result + (columnNameList != null ? columnNameList.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return indexName + "(" + String.join(",", columnNameList) + ")";
    }
}
