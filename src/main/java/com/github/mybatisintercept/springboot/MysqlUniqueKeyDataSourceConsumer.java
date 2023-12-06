package com.github.mybatisintercept.springboot;

import com.github.mybatisintercept.util.PlatformDependentUtil;
import com.github.mybatisintercept.util.TableUniqueIndex;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * 查询唯一索引和主键
 */
public class MysqlUniqueKeyDataSourceConsumer implements Consumer<Collection<DataSource>> {
    private static final Map<Integer, Map<String, List<TableUniqueIndex>>> TABLE_UNIQUE_KEY_COLUMN_MAP_CACHE = new ConcurrentHashMap<>(2);

    @Override
    public void accept(Collection<DataSource> dataSources) {
        if (dataSources == null || dataSources.isEmpty()) {
            return;
        }
        try {
            Map<String, List<TableUniqueIndex>> tableUniqueKeyColumnMap = selectTableUniqueKeyColumnMap(dataSources);
            onSelectEnd(tableUniqueKeyColumnMap);
        } catch (Exception e) {
            Exception exception = onSelectException(e);
            if (exception != null) {
                PlatformDependentUtil.sneakyThrows(exception);
            }
        }
    }

    public void onSelectEnd(Map<String, List<TableUniqueIndex>> tableUniqueKeyColumnMap) {

    }

    public Exception onSelectException(Exception exception) {
        return exception;
    }

    private Map<String, List<TableUniqueIndex>> selectTableUniqueKeyColumnMap(Collection<DataSource> dataSources) {
        Map<String, List<TableUniqueIndex>> map = new LinkedHashMap<>();
        for (DataSource dataSource : dataSources) {
            Integer cacheKey = System.identityHashCode(dataSource);
            Map<String, List<TableUniqueIndex>> rowMap = TABLE_UNIQUE_KEY_COLUMN_MAP_CACHE.computeIfAbsent(cacheKey, k -> {
                String selectCatalog = getCatalog(dataSource);
                Map<String, List<TableUniqueIndex>> m = selectTableUniqueKeyColumnMapByStatistics(dataSource, selectCatalog);
                compressReferenceAndUnmodifiable(m);
                return Collections.unmodifiableMap(m);
            });
            map.putAll(rowMap);
        }
        return map;
    }

    private String getCatalog(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection()) {
            return connection.getCatalog();
        } catch (Exception e) {
            return null;
        }
    }

    private static void compressReferenceAndUnmodifiable(Map<String, List<TableUniqueIndex>> tableUniqueKeyColumnMap) {
        Map<List<TableUniqueIndex>, List<TableUniqueIndex>> cache = new HashMap<>();
        for (Map.Entry<String, List<TableUniqueIndex>> entry : tableUniqueKeyColumnMap.entrySet()) {
            List<TableUniqueIndex> value = entry.getValue();
            List<TableUniqueIndex> tableUniqueIndexCache = cache.computeIfAbsent(value, e -> Collections.unmodifiableList(value));
            if (tableUniqueIndexCache != value) {
                entry.setValue(tableUniqueIndexCache);
            }
        }
    }

    private Map<String, List<TableUniqueIndex>> selectTableUniqueKeyColumnMapByStatistics(DataSource dataSource, String catalog) {
        Map<String, List<TableUniqueIndex>> tableUniqueKeyColumnMap = new LinkedHashMap<>();
        Map<List<String>, List<String>> cache = new HashMap<>();
        Map<TableUniqueIndex, TableUniqueIndex> cache2 = new HashMap<>();
        boolean isCatalog = catalog != null && !catalog.isEmpty();
        String sql = isCatalog ? "SELECT GROUP_CONCAT(DISTINCT TABLE_NAME) TABLE_NAME, GROUP_CONCAT(COLUMN_NAME) COLUMN_NAME, GROUP_CONCAT(DISTINCT INDEX_NAME) INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = ? AND NON_UNIQUE = 0 GROUP BY TABLE_NAME,INDEX_NAME"
                : "SELECT GROUP_CONCAT(DISTINCT TABLE_NAME) TABLE_NAME, GROUP_CONCAT(COLUMN_NAME) COLUMN_NAME, GROUP_CONCAT(DISTINCT INDEX_NAME) INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE NON_UNIQUE = 0 GROUP BY TABLE_NAME,INDEX_NAME";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            if (isCatalog) {
                statement.setObject(1, catalog);
            }
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString(1);
                    String columnName = rs.getString(2);
                    String indexName = rs.getString(3);

                    List<String> uniqueKeyList = Arrays.asList(columnName.split(","));
                    List<String> uniqueKeyListCache = cache.computeIfAbsent(uniqueKeyList, e -> uniqueKeyList);

                    TableUniqueIndex tableUniqueIndex = new TableUniqueIndex(indexName, uniqueKeyListCache);
                    TableUniqueIndex tableUniqueIndexCache = cache2.computeIfAbsent(tableUniqueIndex, e -> tableUniqueIndex);

                    tableUniqueKeyColumnMap.computeIfAbsent(tableName, e -> new ArrayList<>(1))
                            .add(tableUniqueIndexCache);
                }
            }
            return tableUniqueKeyColumnMap;
        } catch (Exception err) {
            // 1044 - Access denied for user
            try {
                return selectTableUniqueKeyColumnMapByInfo(dataSource, catalog);
            } catch (Exception e) {
                PlatformDependentUtil.sneakyThrows(err);
                return Collections.emptyMap();
            }
        }
    }

    private Map<String, List<TableUniqueIndex>> selectTableUniqueKeyColumnMapByInfo(DataSource dataSource, String catalog) {
        Map<String, List<TableUniqueIndex>> tableUniqueKeyColumnMap = new LinkedHashMap<>();
        Map<List<String>, List<String>> cache = new HashMap<>();
        Map<TableUniqueIndex, TableUniqueIndex> cache2 = new HashMap<>();
        boolean isCatalog = catalog != null && !catalog.isEmpty();
        String sql = isCatalog ? "SELECT GROUP_CONCAT(DISTINCT `TABLE_NAME`) TABLE_NAME, GROUP_CONCAT(`COLUMN_NAME`) COLUMN_NAME FROM INFORMATION_SCHEMA.`COLUMNS` WHERE COLUMN_KEY = 'PRI' AND TABLE_SCHEMA = ? GROUP BY TABLE_NAME"
                : "SELECT GROUP_CONCAT(DISTINCT `TABLE_NAME`) TABLE_NAME, GROUP_CONCAT(`COLUMN_NAME`) COLUMN_NAME FROM INFORMATION_SCHEMA.`COLUMNS` WHERE `COLUMN_KEY` = 'PRI' GROUP BY `TABLE_NAME`";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            if (isCatalog) {
                statement.setObject(1, catalog);
            }
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString(1);
                    String columnName = rs.getString(2);

                    List<String> uniqueKeyList = Arrays.asList(columnName.split(","));
                    List<String> uniqueKeyListCache = cache.computeIfAbsent(uniqueKeyList, e -> uniqueKeyList);

                    TableUniqueIndex tableUniqueIndex = new TableUniqueIndex(uniqueKeyListCache);
                    TableUniqueIndex tableUniqueIndexCache = cache2.computeIfAbsent(tableUniqueIndex, e -> tableUniqueIndex);

                    tableUniqueKeyColumnMap.computeIfAbsent(tableName, e -> new ArrayList<>(1))
                            .add(tableUniqueIndexCache);
                }
            }
            return tableUniqueKeyColumnMap;
        } catch (Exception err) {
            // 1044 - Access denied for user
            try {
                return selectTableUniqueKeyColumnMapByShow(dataSource, catalog);
            } catch (Exception e) {
                PlatformDependentUtil.sneakyThrows(err);
                return Collections.emptyMap();
            }
        }
    }

    private Map<String, List<TableUniqueIndex>> selectTableUniqueKeyColumnMapByShow(DataSource dataSource, String catalog) {
        Map<String, List<TableUniqueIndex>> tableUniqueKeyColumnMap = new LinkedHashMap<>();
        Map<List<String>, List<String>> cache = new HashMap<>();
        Map<TableUniqueIndex, TableUniqueIndex> cache2 = new HashMap<>();
        boolean isCatalog = catalog != null && !catalog.isEmpty();
        String sql = isCatalog ? "SHOW TABLE STATUS FROM " + catalog : "SHOW TABLE STATUS";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                List<String> tableNameList = new ArrayList<>();
                while (rs.next()) {
                    tableNameList.add(rs.getString(1));
                }
                DatabaseMetaData metaData = rs.getStatement().getConnection().getMetaData();
                for (String table : tableNameList) {
                    List<String> primaryKeyList = new ArrayList<>(1);
                    String catalogget = "".equals(catalog) ? null : catalog;
                    try (ResultSet primaryKeys = metaData.getPrimaryKeys(catalogget, catalogget, table)) {
                        while (primaryKeys.next()) {
                            primaryKeyList.add(primaryKeys.getString("COLUMN_NAME"));
                        }
                    }
                    List<String> uniqueKeyListCache = cache.computeIfAbsent(primaryKeyList, e -> primaryKeyList);

                    TableUniqueIndex tableUniqueIndex = new TableUniqueIndex(uniqueKeyListCache);
                    TableUniqueIndex tableUniqueIndexCache = cache2.computeIfAbsent(tableUniqueIndex, e -> tableUniqueIndex);

                    tableUniqueKeyColumnMap.computeIfAbsent(table, e -> new ArrayList<>(1))
                            .add(tableUniqueIndexCache);
                }
            }
        } catch (Exception e) {
            PlatformDependentUtil.sneakyThrows(e);
        }
        return tableUniqueKeyColumnMap;
    }
}
