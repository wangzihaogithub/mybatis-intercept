package com.github.mybatisintercept.springboot;

import com.github.mybatisintercept.util.PlatformDependentUtil;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * 查询没有字段的表
 */
public class MysqlMissColumnDataSourceConsumer implements Consumer<Collection<DataSource>> {
    private static final Map<String, Set<String>> SKIP_TABLE_NAMES_CACHE = new ConcurrentHashMap<>(2);
    private static final Map<Integer, String> CATALOG_CACHE = new ConcurrentHashMap<>(2);

    private final TreeSet<TreeSet<String>> columnList = new TreeSet<>(Comparator.comparing(Objects::hashCode));

    public MysqlMissColumnDataSourceConsumer(Collection<? extends Collection<String>> groupColumnList) {
        for (Collection<String> columnList : groupColumnList) {
            this.columnList.add(new TreeSet<>(columnList));
        }
    }

    @Override
    public void accept(Collection<DataSource> dataSources) {
        if (dataSources == null || dataSources.isEmpty()) {
            return;
        }
        try {
            Set<String> missColumnTableList = selectMissColumnTableList(dataSources, columnList);
            onSelectEnd(missColumnTableList);
        } catch (Exception e) {
            Exception exception = onSelectException(e);
            if (exception != null) {
                PlatformDependentUtil.sneakyThrows(exception);
            }
        }
    }

    public void onSelectEnd(Set<String> missColumnTableList) {

    }

    public Exception onSelectException(Exception exception) {
        return exception;
    }

    private Set<String> selectMissColumnTableList(Collection<DataSource> dataSources, Collection<? extends Collection<String>> columnNameListList) {
        Set<String> tableList = new LinkedHashSet<>();
        if (columnNameListList == null || columnNameListList.isEmpty()) {
            return tableList;
        }
        for (DataSource dataSource : dataSources) {
            String selectCatalog = getCatalog(dataSource);
            List<String> itemTableList = selectMissColumnTableList(dataSource, selectCatalog, columnNameListList);
            tableList.addAll(itemTableList);
        }
        return tableList;
    }

    private String getCatalog(DataSource dataSource) {
        return CATALOG_CACHE.computeIfAbsent(System.identityHashCode(dataSource), e -> {
            try (Connection connection = dataSource.getConnection()) {
                return connection.getCatalog();
            } catch (Exception err) {
                return null;
            }
        });
    }

    private List<String> selectMissColumnTableList(DataSource dataSource, String catalog, Collection<? extends Collection<String>> columnNameListList) {
        // columnNameListList两种情况都没有，才算miss
        Map<String, Integer> missCounterMap = new LinkedHashMap<>();
        for (Collection<String> columnNameList : columnNameListList) {
            String cacheKey = String.format("%d-%s", System.identityHashCode(dataSource), Objects.hash(columnNameList));
            Set<String> tableList = SKIP_TABLE_NAMES_CACHE.computeIfAbsent(cacheKey, e -> {
                Set<String> list = new LinkedHashSet<>();
                int columnNameSize = columnNameList.size();
                StringJoiner args = new StringJoiner(",");
                for (int i = 0; i < columnNameSize; i++) {
                    args.add("?");
                }
                boolean isCatalog = catalog != null && !catalog.isEmpty();
                String sql = isCatalog ? "SELECT GROUP_CONCAT(DISTINCT TABLE_NAME) TABLE_NAME,COUNT(IF(COLUMN_NAME in (" + args + "),1,null)) CNT FROM INFORMATION_SCHEMA.`COLUMNS` WHERE TABLE_SCHEMA = ? GROUP BY TABLE_NAME HAVING CNT != ?"
                        : "SELECT GROUP_CONCAT(DISTINCT TABLE_NAME) TABLE_NAME,COUNT(IF(COLUMN_NAME in (" + args + "),1,null)) CNT FROM INFORMATION_SCHEMA.`COLUMNS` GROUP BY TABLE_NAME HAVING CNT != ?";
                try (Connection connection = dataSource.getConnection();
                     PreparedStatement statement = connection.prepareStatement(sql)) {
                    int parameterIndex = 0;
                    for (String columnName : columnNameList) {
                        statement.setString(++parameterIndex, columnName);
                    }
                    if (isCatalog) {
                        statement.setString(++parameterIndex, catalog);
                    }
                    statement.setInt(++parameterIndex, columnNameSize);

                    try (ResultSet rs = statement.executeQuery()) {
                        while (rs.next()) {
                            String tableName = rs.getString(1);
                            list.add(tableName);
                        }
                    }
                } catch (Exception err) {
                    PlatformDependentUtil.sneakyThrows(err);
                }
                return Collections.unmodifiableSet(list);
            });

            for (String tableName : tableList) {
                missCounterMap.compute(tableName, (k, v) -> v == null ? 1 : v + 1);
            }
        }
        return missCounterMap.entrySet().stream().filter(e -> e.getValue() == columnNameListList.size()).map(Map.Entry::getKey).collect(Collectors.toList());
    }

}
