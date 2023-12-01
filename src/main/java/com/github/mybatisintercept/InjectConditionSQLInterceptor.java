package com.github.mybatisintercept;

import com.github.mybatisintercept.util.*;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * 自动给（select语句, update语句, delete语句, insert from语句）加条件
 *
 * @author wangzihaogithub 2022-11-04
 */
@Intercepts({
        @Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class,
                RowBounds.class, ResultHandler.class}),
        @Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class,
                RowBounds.class, ResultHandler.class, CacheKey.class, BoundSql.class}),
        @Signature(type = Executor.class, method = "queryCursor", args = {MappedStatement.class, Object.class,
                RowBounds.class}),
        @Signature(type = Executor.class, method = "update", args = {MappedStatement.class, Object.class})})
public class InjectConditionSQLInterceptor implements Interceptor {
    private static final List<InjectConditionSQLInterceptor> INSTANCE_LIST = Collections.synchronizedList(new LinkedList<>());
    private final Set<String> interceptPackageNames = new LinkedHashSet<>();
    private final Set<String> skipTableNames = new LinkedHashSet<>();
    private final Map<String, List<TableUniqueIndex>> tableUniqueKeyColumnMap = Collections.synchronizedMap(new HashMap<>());
    private final AtomicBoolean initFlag = new AtomicBoolean();
    private String dbType;
    private ASTDruidConditionUtil.ExistInjectConditionStrategyEnum existInjectConditionStrategyEnum;
    private SQL conditionExpression;
    private StaticMethodAccessor<InterceptContext> valueProvider;
    private BiPredicate<String, String> skipPredicate = (schema, tableName) -> {
        return skipTableNames.contains(tableName);
    };
    private Predicate<SQLCondition> uniqueKeyPredicate = sqlCondition -> {
        return sqlCondition.isCanIgnoreInject(tableUniqueKeyColumnMap);
    };
    private Properties properties;

    public InjectConditionSQLInterceptor() {
        INSTANCE_LIST.add(this);
    }

    public static InterceptContext getInterceptContext() {
        return StaticMethodAccessor.getContext(InterceptContext.class);
    }

    public static List<InjectConditionSQLInterceptor> getInstanceList() {
        return Collections.unmodifiableList(INSTANCE_LIST);
    }

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        // Spring bean 方式配置时，如果没有配置属性就不会执行下面的 setProperties 方法，就不会初始化 因此这里会出现 null 的情况
        initIfNeed();

        InterceptContext interceptContext = new InterceptContext(invocation, this);
        if (isSupportIntercept(interceptContext)) {
            String rawSql = MybatisUtil.getBoundSqlString(invocation);
            String injectCondition = compileInject(interceptContext);
            String newSql = ASTDruidUtil.addAndCondition(rawSql, injectCondition, existInjectConditionStrategyEnum, dbType, skipPredicate, uniqueKeyPredicate);
            if (!Objects.equals(rawSql, newSql)) {
                MybatisUtil.rewriteSql(invocation, newSql);
            }
        }
        return invocation.proceed();
    }

    protected String compileInject(InterceptContext interceptContext) {
        SQL sql = SQL.compile(conditionExpression.getSourceSql(), name -> valueProvider.invokeWithOnBindContext(name, interceptContext));
        return sql.getExprSql();
    }

    protected boolean isSupportIntercept(InterceptContext interceptContext) {
        return MybatisUtil.isInterceptPackage(interceptContext.invocation, interceptPackageNames)
                && existExpressionValue(interceptContext)
                && ASTDruidUtil.isSingleStatementAndSupportWhere(MybatisUtil.getBoundSqlString(interceptContext.invocation), dbType);
    }

    protected boolean existExpressionValue(InterceptContext interceptContext) {
        for (SQL.Placeholder placeholder : conditionExpression.getPlaceholders()) {
            Object value = valueProvider.invokeWithOnBindContext(placeholder.getArgName(), interceptContext);
            if (value == null) {
                return false;
            }
        }
        return true;
    }

    public Set<String> getSkipTableNames() {
        return skipTableNames;
    }

    public Map<String, List<TableUniqueIndex>> getTableUniqueKeyColumnMap() {
        return tableUniqueKeyColumnMap;
    }

    public Predicate<SQLCondition> getUniqueKeyPredicate() {
        return uniqueKeyPredicate;
    }

    public void setUniqueKeyPredicate(Predicate<SQLCondition> uniqueKeyPredicate) {
        this.uniqueKeyPredicate = uniqueKeyPredicate;
    }

    public BiPredicate<String, String> getSkipPredicate() {
        return skipPredicate;
    }

    public StaticMethodAccessor<InterceptContext> getValueProvider() {
        return valueProvider;
    }

    public String getDbType() {
        return dbType;
    }

    public SQL getConditionExpression() {
        return conditionExpression;
    }

    public Set<String> getInterceptPackageNames() {
        return interceptPackageNames;
    }

    public void setValueProvider(StaticMethodAccessor<InterceptContext> valueProvider) {
        this.valueProvider = valueProvider;
    }

    public void setSkipPredicate(BiPredicate<String, String> skipPredicate) {
        this.skipPredicate = skipPredicate;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public void setConditionExpression(SQL conditionExpression) {
        this.conditionExpression = conditionExpression;
    }

    public ASTDruidConditionUtil.ExistInjectConditionStrategyEnum getExistInjectConditionStrategyEnum() {
        return existInjectConditionStrategyEnum;
    }

    public void setExistInjectConditionStrategyEnum(ASTDruidConditionUtil.ExistInjectConditionStrategyEnum existInjectConditionStrategyEnum) {
        this.existInjectConditionStrategyEnum = existInjectConditionStrategyEnum;
    }

    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
        if (PlatformDependentUtil.EXIST_SPRING_BOOT) {
            PlatformDependentUtil.onSpringEnvironmentReady(this::initIfNeed);
        }
    }

    public void initIfNeed() {
        if (!initFlag.compareAndSet(false, true)) {
            return;
        }
        Properties properties = this.properties;
        if (properties == null || properties.isEmpty()) {
            properties = System.getProperties();
        }
        if (PlatformDependentUtil.SPRING_ENVIRONMENT_READY) {
            properties = PlatformDependentUtil.resolveSpringPlaceholders(properties, "InjectConditionSQLInterceptor.");
        }
        String valueProvider = properties.getProperty("InjectConditionSQLInterceptor.valueProvider", "com.github.securityfilter.util.AccessUserUtil#getAccessUserValue");
        String dbType = properties.getProperty("InjectConditionSQLInterceptor.dbType", "mysql");
        String existInjectConditionStrategyEnum = properties.getProperty("InjectConditionSQLInterceptor.existInjectConditionStrategyEnum", "RULE_TABLE_MATCH_THEN_SKIP_ITEM");

        String conditionExpression = properties.getProperty("InjectConditionSQLInterceptor.conditionExpression", "tenant_id = ${tenantId}"); // 字符串请这样写： 字段 = '${属性}'
        String interceptPackageNames = properties.getProperty("InjectConditionSQLInterceptor.interceptPackageNames", ""); // 空字符=不限制，全拦截
        String skipTableNames = properties.getProperty("InjectConditionSQLInterceptor.skipTableNames", "");

        this.conditionExpression = SQL.compile(conditionExpression, Collections.emptyMap());

        boolean enabledUniqueKey = "true".equalsIgnoreCase(properties.getProperty("InjectConditionSQLInterceptor.enabledUniqueKey", "true"));
        if (enabledUniqueKey) {
            // uniqueKey解析格式：table1=col1,col2;table2=col3;table3=col5,col6
            String tableUniqueKeyColumn = properties.getProperty("InjectConditionSQLInterceptor.uniqueKey", "");
            this.tableUniqueKeyColumnMap.putAll(parseTableUniqueKeyColumnMap(Arrays.asList(tableUniqueKeyColumn.split(";"))));

            if (PlatformDependentUtil.EXIST_SPRING_BOOT && "true".equalsIgnoreCase(properties.getProperty("InjectConditionSQLInterceptor.enabledDatasourceSelect", "true"))) {
                if ("mysql".equalsIgnoreCase(dbType) || "mariadb".equalsIgnoreCase(dbType)) {
                    PlatformDependentUtil.onSpringDatasourceReady(new MysqlDataSourcesConsumer(this.tableUniqueKeyColumnMap, this.skipTableNames, this.conditionExpression));
                }
            }
        }

        this.valueProvider = new StaticMethodAccessor<>(valueProvider);
        this.dbType = dbType;
        this.existInjectConditionStrategyEnum = ASTDruidConditionUtil.ExistInjectConditionStrategyEnum.valueOf(existInjectConditionStrategyEnum);
        if (interceptPackageNames.trim().length() > 0) {
            this.interceptPackageNames.addAll(Arrays.stream(interceptPackageNames.trim().split(",")).map(String::trim).collect(Collectors.toList()));
        }
        if (skipTableNames.trim().length() > 0) {
            this.skipTableNames.addAll(Arrays.stream(skipTableNames.trim().split(",")).map(String::trim).collect(Collectors.toList()));
        }
    }

    static class MysqlDataSourcesConsumer implements Consumer<Collection<DataSource>> {
        private final Map<String, List<TableUniqueIndex>> tableUniqueKeyColumnMap;
        private final Set<String> skipTableNames;
        private final SQL conditionExpression;

        private MysqlDataSourcesConsumer(Map<String, List<TableUniqueIndex>> tableUniqueKeyColumnMap, Set<String> skipTableNames, SQL conditionExpression) {
            this.tableUniqueKeyColumnMap = tableUniqueKeyColumnMap;
            this.skipTableNames = skipTableNames;
            this.conditionExpression = conditionExpression;
        }

        @Override
        public void accept(Collection<DataSource> dataSources) {
            if (dataSources == null || dataSources.isEmpty()) {
                return;
            }
            Map<String, List<TableUniqueIndex>> tableUniqueKeyColumnMap = selectTableUniqueKeyColumnMap(dataSources);
            for (Map.Entry<String, List<TableUniqueIndex>> entry : tableUniqueKeyColumnMap.entrySet()) {
                this.tableUniqueKeyColumnMap.putIfAbsent(entry.getKey(), entry.getValue());
            }

            SQL compile = SQL.compile(conditionExpression.getSourceSql(), e -> "?");
            Set<String> columnList = new LinkedHashSet<>(ASTDruidConditionUtil.getColumnList(compile.getExprSql()));
            Set<String> missColumnTableList = selectMissColumnTableList(dataSources, columnList);
            this.skipTableNames.addAll(missColumnTableList);
        }

        private Map<String, List<TableUniqueIndex>> selectTableUniqueKeyColumnMap(Collection<DataSource> dataSources) {
            Map<String, List<TableUniqueIndex>> map = new LinkedHashMap<>();
            for (DataSource dataSource : dataSources) {
                try {
                    String selectCatalog = getCatalog(dataSource);
                    Map<String, List<TableUniqueIndex>> rowMap = selectTableUniqueKeyColumnMapByStatistics(dataSource, selectCatalog);
                    map.putAll(rowMap);
                } catch (Exception ignored) {

                }
            }
            return map;
        }

        private Set<String> selectMissColumnTableList(Collection<DataSource> dataSources, Collection<String> columnNameList) {
            Set<String> tableList = new LinkedHashSet<>();
            if (columnNameList == null || columnNameList.isEmpty()) {
                return tableList;
            }
            for (DataSource dataSource : dataSources) {
                try {
                    String selectCatalog = getCatalog(dataSource);
                    List<String> rowTableList = selectMissColumnTableList(dataSource, selectCatalog, columnNameList);
                    tableList.addAll(rowTableList);
                } catch (Exception ignored) {

                }
            }
            return tableList;
        }

        private String getCatalog(DataSource dataSource) {
            try (Connection connection = dataSource.getConnection()) {
                return connection.getCatalog();
            } catch (Exception e) {
                return null;
            }
        }

        private List<String> selectMissColumnTableList(DataSource dataSource, String catalog, Collection<String> columnNameList) {
            List<String> tableList = new ArrayList<>();
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
                        tableList.add(tableName);
                    }
                }
            } catch (Exception ignored) {
                // 1044 - Access denied for user
            }
            return tableList;
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
            } catch (Exception ignored) {
                // 1044 - Access denied for user
                return selectTableUniqueKeyColumnMapByInfo(dataSource, catalog);
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
            } catch (Exception ignored) {
                // 1044 - Access denied for user
                return selectTableUniqueKeyColumnMapByShow(dataSource, catalog);
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
            } catch (Exception ignored) {
                // 1044 - Access denied for user
            }
            return tableUniqueKeyColumnMap;
        }
    }


    /**
     * table=col1,col2
     */
    private static Map<String, List<TableUniqueIndex>> parseTableUniqueKeyColumnMap(List<String> list) {
        Map<String, List<TableUniqueIndex>> map = new HashMap<>();
        for (String string : list) {
            String[] split = string.split("=", 2);
            if (split.length != 2) {
                continue;
            }
            String tableName = split[0].trim();
            String cols = split[1].trim();
            String[] colsArray = cols.split(",");
            map.put(tableName, new ArrayList<>(Collections.singletonList(new TableUniqueIndex(Arrays.stream(colsArray).map(String::trim).collect(Collectors.toList())))));
        }
        return map;
    }

    public static class InterceptContext implements com.github.mybatisintercept.InterceptContext<InjectConditionSQLInterceptor> {
        private final Invocation invocation;
        private final InjectConditionSQLInterceptor interceptor;

        public InterceptContext(Invocation invocation, InjectConditionSQLInterceptor interceptor) {
            this.invocation = invocation;
            this.interceptor = interceptor;
        }

        @Override
        public InjectConditionSQLInterceptor getInterceptor() {
            return interceptor;
        }

        @Override
        public Invocation getInvocation() {
            return invocation;
        }

    }

}
