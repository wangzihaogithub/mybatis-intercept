package com.github.mybatisintercept;

import com.github.mybatisintercept.util.*;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
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
    private static final ThreadLocal<Integer[]> SELECTOR_THREAD_LOCAL = new ThreadLocal<>();
    private static final List<InjectConditionSQLInterceptor> INSTANCE_LIST = Collections.synchronizedList(new LinkedList<>());
    private final Set<String> interceptPackageNames = new LinkedHashSet<>();
    /**
     * key = conditionExpression
     * value = ConditionSkipTablePredicate
     */
    private final Map<String, ConditionSkipTablePredicate> skipTablePredicateMap = new ConcurrentHashMap<>(3);
    private final SkipTablePredicate defaultSkipTablePredicate = new SkipTablePredicate();
    /**
     * key = 表名
     * value = 唯一索引集合
     */
    private final Map<String, List<TableUniqueIndex>> tableUniqueKeyColumnMap = Collections.synchronizedMap(new HashMap<>());
    private final List<SQL> conditionExpressionList = new ArrayList<>();
    private final List<SQL> unmodifiableConditionExpressionList = Collections.unmodifiableList(conditionExpressionList);
    private final AtomicBoolean initFlag = new AtomicBoolean();
    private String dbType;
    private ExistInjectConditionStrategyEnum existInjectConditionStrategyEnum;
    private StaticMethodAccessor<InterceptContext> valueProvider;
    private Predicate<SQLCondition> uniqueKeyPredicate = sqlCondition -> sqlCondition.isCanIgnoreInject(tableUniqueKeyColumnMap);
    private CompileConditionInjectSelector compileConditionInjectSelector = CompileConditionInjectSelector.DEFAULT;
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

    public static List<SQL> indexOf(List<SQL> sqlList, Integer... indexes) {
        if (indexes == null || indexes.length == 0) {
            return Collections.emptyList();
        } else {
            List<SQL> list = new ArrayList<>(indexes.length);
            for (Integer index : indexes) {
                list.add(sqlList.get(index));
            }
            return list;
        }
    }

    public static InjectSelector openSelector() {
        return new InjectSelector();
    }

    public static void setSelector(Integer... index) {
        if (index == null || index.length == 0) {
            SELECTOR_THREAD_LOCAL.remove();
        } else {
            SELECTOR_THREAD_LOCAL.set(index);
        }
    }

    public static void removeSelector() {
        SELECTOR_THREAD_LOCAL.remove();
    }

    public static Integer[] getSelector() {
        return SELECTOR_THREAD_LOCAL.get();
    }

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        // Spring bean 方式配置时，如果没有配置属性就不会执行下面的 setProperties 方法，就不会初始化 因此这里会出现 null 的情况
        initIfNeed();

        // 1. isInterceptPackage
        if (MybatisUtil.isInterceptPackage(invocation, interceptPackageNames)) {
            // 2. isSupportIntercept
            int index = 0;
            InterceptContext interceptContext = new InterceptContext(invocation, this, index);
            if (isSupportIntercept(interceptContext)) {
                String rawSql = MybatisUtil.getBoundSqlString(invocation);
                do {
                    // 3. compileInject
                    String injectCondition = compileConditionInjectSelector.compileInject(interceptContext);
                    if (!interceptContext.isStop() && injectCondition != null && !injectCondition.isEmpty()) {
                        // 4. addAndCondition
                        String currRawSql = index == 0 ? rawSql : MybatisUtil.getBoundSqlString(invocation);
                        String newSql = compileConditionInjectSelector.addAndCondition(rawSql, currRawSql, injectCondition, interceptContext);
                        if (!interceptContext.isStop() && !Objects.equals(currRawSql, newSql)) {
                            // 5. rewriteSql
                            MybatisUtil.rewriteSql(invocation, newSql);
                        }
                    }
                } while (interceptContext.next(++index) && !interceptContext.isStop());
            }
        }
        return invocation.proceed();
    }

    protected boolean isSupportIntercept(InterceptContext interceptContext) {
        return ASTDruidUtil.isSingleStatementAndSupportWhere(MybatisUtil.getBoundSqlString(interceptContext.invocation), dbType);
    }

    public SkipTablePredicate getDefaultSkipTablePredicate() {
        return defaultSkipTablePredicate;
    }

    public List<SQL> getUnmodifiableConditionExpressionList() {
        return unmodifiableConditionExpressionList;
    }

    public List<SQL> getConditionExpressionList() {
        return conditionExpressionList;
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

    public StaticMethodAccessor<InterceptContext> getValueProvider() {
        return valueProvider;
    }

    public String getDbType() {
        return dbType;
    }

    public Set<String> getInterceptPackageNames() {
        return interceptPackageNames;
    }

    public void setValueProvider(StaticMethodAccessor<InterceptContext> valueProvider) {
        this.valueProvider = valueProvider;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public ExistInjectConditionStrategyEnum getExistInjectConditionStrategyEnum() {
        return existInjectConditionStrategyEnum;
    }

    public void setExistInjectConditionStrategyEnum(ExistInjectConditionStrategyEnum existInjectConditionStrategyEnum) {
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

    private Properties getProperties() {
        Properties result = this.properties;
        if (result == null || result.isEmpty()) {
            result = System.getProperties();
        }
        if (PlatformDependentUtil.SPRING_ENVIRONMENT_READY) {
            result = PlatformDependentUtil.resolveSpringPlaceholders(result, "InjectConditionSQLInterceptor.");
        }
        return result;
    }

    public void initIfNeed() {
        if (!initFlag.compareAndSet(false, true)) {
            return;
        }
        Properties properties = getProperties();
        String valueProviderString = properties.getProperty("InjectConditionSQLInterceptor.valueProvider", "com.github.securityfilter.util.AccessUserUtil#getAccessUserValue");
        String dbTypeString = properties.getProperty("InjectConditionSQLInterceptor.dbType", "mysql");
        String existInjectConditionStrategyEnumString = properties.getProperty("InjectConditionSQLInterceptor.existInjectConditionStrategyEnum", "RULE_TABLE_MATCH_THEN_SKIP_ITEM");

        String[] conditionExpressions = filterEmpty(properties.getProperty("InjectConditionSQLInterceptor.conditionExpression", "tenant_id = ${tenantId}").split(";")); // 字符串请这样写： 字段 = '${属性}'
        String interceptPackageNamesString = properties.getProperty("InjectConditionSQLInterceptor.interceptPackageNames", ""); // 空字符=不限制，全拦截
        String skipTableNames = properties.getProperty("InjectConditionSQLInterceptor.skipTableNames", "");
        boolean enabledUniqueKey = "true".equalsIgnoreCase(properties.getProperty("InjectConditionSQLInterceptor.enabledUniqueKey", "true"));
        boolean enabledDatasourceSelect = "true".equalsIgnoreCase(properties.getProperty("InjectConditionSQLInterceptor.datasourceSelect", "true"));
        boolean datasourceSelectErrorThenShutdown = "true".equalsIgnoreCase(properties.getProperty("InjectConditionSQLInterceptor.datasourceSelectErrorThenShutdown", "true"));

        this.conditionExpressionList.addAll(Arrays.stream(conditionExpressions).map(e -> SQL.compile(e, Collections.emptyMap())).collect(Collectors.toList()));

        if (PlatformDependentUtil.EXIST_SPRING_BOOT && enabledDatasourceSelect && PlatformDependentUtil.isMysql(dbTypeString)) {
            for (String conditionExpression : conditionExpressions) {
                SQL compile = SQL.compile(conditionExpression, k -> "?");
                List<String> columnList = ASTDruidConditionUtil.getColumnList(compile.getExprSql());
                PlatformDependentUtil.onSpringDatasourceReady(new MysqlMissColumnDataSourceConsumer(Collections.singletonList(columnList)) {
                    @Override
                    public void onSelectEnd(Set<String> missColumnTableList) {
                        ConditionSkipTablePredicate predicate = new ConditionSkipTablePredicate(conditionExpression, columnList);
                        predicate.getSkipTableNames().addAll(missColumnTableList);
                        InjectConditionSQLInterceptor.this.skipTablePredicateMap.put(conditionExpression, predicate);
                    }

                    @Override
                    public Exception onSelectException(Exception exception) {
                        if (datasourceSelectErrorThenShutdown) {
                            PlatformDependentUtil.onSpringDatasourceReady(unused -> System.exit(-1));
                        }
                        return new IllegalStateException("InjectConditionSQLInterceptor.skipTableNames init fail! if dont need shutdown can setting InjectConditionSQLInterceptor.datasourceSelectErrorThenShutdown = false, InjectColumnValuesUpdateSQLInterceptor.datasourceSelectErrorThenShutdown = false, InjectColumnValuesInsertSQLInterceptor.datasourceSelectErrorThenShutdown = false. case:" + exception, exception);
                    }
                });
            }
        }

        if (enabledUniqueKey) {
            // uniqueKey解析格式：table1=col1,col2;table2=col3;table3=col5,col6
            String tableUniqueKeyColumn = properties.getProperty("InjectConditionSQLInterceptor.uniqueKey", "");
            this.tableUniqueKeyColumnMap.putAll(parseTableUniqueKeyColumnMap(Arrays.asList(tableUniqueKeyColumn.split(";"))));

            if (PlatformDependentUtil.EXIST_SPRING_BOOT && enabledDatasourceSelect && PlatformDependentUtil.isMysql(dbTypeString)) {
                PlatformDependentUtil.onSpringDatasourceReady(new MysqlUniqueKeyDataSourceConsumer() {
                    @Override
                    public void onSelectEnd(Map<String, List<TableUniqueIndex>> tableUniqueKeyColumnMap) {
                        for (Map.Entry<String, List<TableUniqueIndex>> entry : tableUniqueKeyColumnMap.entrySet()) {
                            InjectConditionSQLInterceptor.this.tableUniqueKeyColumnMap.putIfAbsent(entry.getKey(), entry.getValue());
                        }
                    }

                    @Override
                    public Exception onSelectException(Exception exception) {
                        if (datasourceSelectErrorThenShutdown) {
                            PlatformDependentUtil.onSpringDatasourceReady(unused -> System.exit(-1));
                        }
                        return new IllegalStateException("InjectConditionSQLInterceptor.uniqueKey. init fail! if dont need shutdown can setting InjectConditionSQLInterceptor.datasourceSelectErrorThenShutdown = false, InjectColumnValuesUpdateSQLInterceptor.datasourceSelectErrorThenShutdown = false, InjectColumnValuesInsertSQLInterceptor.datasourceSelectErrorThenShutdown = false. case:" + exception, exception);
                    }
                });
            }
        }

        PlatformDependentUtil.onCompileInjectorReady(e -> {
            this.compileConditionInjectSelector = e;
            e.init(this);
        });

        this.valueProvider = new StaticMethodAccessor<>(valueProviderString);
        this.dbType = dbTypeString;
        this.existInjectConditionStrategyEnum = ExistInjectConditionStrategyEnum.valueOf(existInjectConditionStrategyEnumString);
        if (interceptPackageNamesString.trim().length() > 0) {
            this.interceptPackageNames.addAll(Arrays.stream(interceptPackageNamesString.trim().split(",")).map(String::trim).collect(Collectors.toList()));
        }
        if (skipTableNames.trim().length() > 0) {
            this.defaultSkipTablePredicate.skipTableNames.addAll(Arrays.stream(skipTableNames.trim().split(",")).map(String::trim).filter(e -> !e.isEmpty()).collect(Collectors.toList()));
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
            if (tableName.isEmpty()) {
                continue;
            }
            String cols = split[1].trim();
            if (cols.isEmpty()) {
                continue;
            }
            String[] colsArray = cols.split(",");
            map.put(tableName, new ArrayList<>(Collections.singletonList(new ParseTableUniqueIndex(Arrays.stream(colsArray).map(String::trim).collect(Collectors.toList())))));
        }
        return map;
    }

    public static class ParseTableUniqueIndex extends TableUniqueIndex {
        public ParseTableUniqueIndex(List<String> columnNameList) {
            super(columnNameList);
        }
    }

    public static class InterceptContext implements com.github.mybatisintercept.InterceptContext<InjectConditionSQLInterceptor> {
        private final Invocation invocation;
        private final InjectConditionSQLInterceptor interceptor;
        private String dbType;
        private List<SQL> conditionExpressionList;
        private final List<String> existInjectConditionList = new ArrayList<>(1);
        private int conditionExpressionIndex;
        private Map<String, Object> attributeMap;
        private boolean stop;

        // once
        private ExistInjectConditionStrategyEnum existInjectConditionStrategyEnum;
        private UnaryOperator<BiPredicate<String, String>> skipPredicateWrapper;
        private UnaryOperator<Predicate<SQLCondition>> uniqueKeyPredicateWrapper;

        public InterceptContext(Invocation invocation, InjectConditionSQLInterceptor interceptor, int conditionExpressionIndex) {
            this.invocation = invocation;
            this.interceptor = interceptor;
            this.existInjectConditionStrategyEnum = interceptor.getExistInjectConditionStrategyEnum();
            this.dbType = interceptor.getDbType();
            this.conditionExpressionIndex = conditionExpressionIndex;
        }

        void reset() {
            this.existInjectConditionStrategyEnum = interceptor.getExistInjectConditionStrategyEnum();
            this.skipPredicateWrapper = null;
            this.uniqueKeyPredicateWrapper = null;
        }

        public List<String> getExistInjectConditionList() {
            return existInjectConditionList;
        }

        public void stop(boolean stop) {
            this.stop = stop;
        }

        public boolean isStop() {
            return stop;
        }

        public String getDbType() {
            return dbType;
        }

        public void setDbType(String dbType) {
            this.dbType = dbType;
        }

        public ExistInjectConditionStrategyEnum getExistInjectConditionStrategyEnum() {
            return existInjectConditionStrategyEnum;
        }

        public void setExistInjectConditionStrategyEnum(ExistInjectConditionStrategyEnum existInjectConditionStrategyEnum) {
            this.existInjectConditionStrategyEnum = existInjectConditionStrategyEnum;
        }

        public SQL getConditionExpression() {
            return conditionExpressionList == null || conditionExpressionList.isEmpty() || conditionExpressionIndex >= conditionExpressionList.size() ? null : conditionExpressionList.get(conditionExpressionIndex);
        }

        public int getConditionExpressionIndexId() {
            return getConditionExpressionIndexId(getConditionExpression());
        }

        public int getConditionExpressionIndexId(SQL conditionExpression) {
            return conditionExpression == null ? -1 : interceptor.getConditionExpressionList().indexOf(conditionExpression);
        }

        public List<SQL> getConditionExpressionList() {
            return conditionExpressionList;
        }

        public void setConditionExpressionList(List<SQL> conditionExpressionList) {
            this.conditionExpressionList = conditionExpressionList;
        }

        public int getConditionExpressionIndex() {
            return conditionExpressionIndex;
        }

        boolean next(int index) {
            this.conditionExpressionIndex = index;
            return conditionExpressionList != null && index < conditionExpressionList.size();
        }

        public SkipTablePredicate getConditionSkipPredicate() {
            SQL conditionExpression = getConditionExpression();
            return conditionExpression == null ? null : interceptor.skipTablePredicateMap.get(conditionExpression.getSourceSql());
        }

        public void setSkipPredicateWrapper(UnaryOperator<BiPredicate<String, String>> skipPredicateWrapper) {
            this.skipPredicateWrapper = skipPredicateWrapper;
        }

        public void setUniqueKeyPredicateWrapper(UnaryOperator<Predicate<SQLCondition>> uniqueKeyPredicateWrapper) {
            this.uniqueKeyPredicateWrapper = uniqueKeyPredicateWrapper;
        }

        public Predicate<SQLCondition> getUniqueKeyPredicate() {
            Predicate<SQLCondition> uniqueKeyPredicate = interceptor.getUniqueKeyPredicate();
            return uniqueKeyPredicateWrapper != null ? uniqueKeyPredicateWrapper.apply(uniqueKeyPredicate) : uniqueKeyPredicate;
        }

        private BiPredicate<String, String> getSkipPredicate() {
            BiPredicate<String, String> biPredicate = new LogicalOr(getConditionSkipPredicate(), interceptor.defaultSkipTablePredicate);
            return skipPredicateWrapper != null ? skipPredicateWrapper.apply(biPredicate) : biPredicate;
        }

        @Override
        public InjectConditionSQLInterceptor getInterceptor() {
            return interceptor;
        }

        @Override
        public Invocation getInvocation() {
            return invocation;
        }

        @Override
        public Map<String, Object> getAttributeMap() {
            if (attributeMap == null) {
                attributeMap = new HashMap<>(2);
            }
            return attributeMap;
        }

        @Override
        public Object getAttributeValue(String name) {
            return attributeMap == null ? null : attributeMap.get(name);
        }

        @Override
        public StaticMethodAccessor<com.github.mybatisintercept.InterceptContext<InjectConditionSQLInterceptor>> getValueProvider() {
            return (StaticMethodAccessor) interceptor.valueProvider;
        }
    }

    public interface CompileConditionInjectSelector {
        CompileConditionInjectSelector DEFAULT = (conditionExpressionList, valueGetter, interceptContext) -> conditionExpressionList.isEmpty() ? null : Collections.singletonList(conditionExpressionList.get(0));

        /**
         * 初始化
         *
         * @param instance
         */
        default void init(InjectConditionSQLInterceptor instance) {

        }

        default String compileInject(InterceptContext interceptContext) {
            com.github.mybatisintercept.InterceptContext.ValueGetter valueGetter = interceptContext.getValueGetter();
            int index = interceptContext.getConditionExpressionIndex();
            List<SQL> selectConditionExpression;
            if (index == 0) {
                interceptContext.setConditionExpressionList(selectConditionExpression = select(valueGetter, interceptContext));
            } else {
                interceptContext.reset();
                selectConditionExpression = interceptContext.getConditionExpressionList();
            }
            if (selectConditionExpression == null || selectConditionExpression.isEmpty()) {
                return null;
            }
            SQL sql = selectConditionExpression.get(index);
            if (sql == null) {
                return null;
            }
            int sqlIndex = interceptContext.getConditionExpressionIndexId(sql);
            return compile(index, sqlIndex, sql, valueGetter, interceptContext);
        }

        default List<SQL> select(com.github.mybatisintercept.InterceptContext.ValueGetter valueGetter, InterceptContext interceptContext) {
            List<SQL> selectConditionExpression;
            List<SQL> unmodifiableConditionExpressionList = interceptContext.getInterceptor().getUnmodifiableConditionExpressionList();
            Integer[] index = getSelector();
            if (index != null) {
                selectConditionExpression = indexOf(unmodifiableConditionExpressionList, index);
            } else {
                selectConditionExpression = select(unmodifiableConditionExpressionList, valueGetter, interceptContext);
            }
            return selectConditionExpression;
        }

        /**
         * 添加and条件
         *
         * @param rawSql           原始sql
         * @param currRawSql       原始sql
         * @param injectCondition  注入条件
         * @param interceptContext 上下文
         * @return 添加and条件后的sql
         */
        default String addAndCondition(String rawSql, String currRawSql, String injectCondition, InterceptContext interceptContext) {
            List<String> existInjectConditionList = interceptContext.getExistInjectConditionList();
            String sql = ASTDruidUtil.addAndCondition(currRawSql, injectCondition, interceptContext.getExistInjectConditionStrategyEnum(),
                    interceptContext.getDbType(), interceptContext.getSkipPredicate(), interceptContext.getUniqueKeyPredicate(), existInjectConditionList);
            existInjectConditionList.add(injectCondition);
            return sql;
        }

        /**
         * 筛选出SQL模板
         *
         * @param conditionExpressionList 表达式sql集合
         * @param valueGetter             表达式值
         * @param interceptContext        上下文
         * @return 筛选后SQL模板
         */
        List<SQL> select(List<SQL> conditionExpressionList, com.github.mybatisintercept.InterceptContext.ValueGetter valueGetter, InterceptContext interceptContext);

        /**
         * 编译
         *
         * @param conditionExpression 表达式sql
         * @param valueGetter         表达式值
         * @param interceptContext    上下文
         * @return 编译后的SQL
         */
        default String compile(String conditionExpression, com.github.mybatisintercept.InterceptContext.ValueGetter valueGetter, InterceptContext interceptContext) {
            return SQL.compileString(conditionExpression, valueGetter::getValue, true);
        }

        default String compile(int index, int sqlIndex, SQL sql, com.github.mybatisintercept.InterceptContext.ValueGetter valueGetter, InterceptContext interceptContext) {
            return compile(sql.getSourceSql(), valueGetter, interceptContext);
        }
    }

    public static class SkipTablePredicate implements BiPredicate<String, String> {
        private final Set<String> skipTableNames = Collections.newSetFromMap(new ConcurrentHashMap<>());

        @Override
        public boolean test(String schema, String tableName) {
            return skipTableNames.contains(tableName);
        }

        public Set<String> getSkipTableNames() {
            return skipTableNames;
        }
    }

    public static class ConditionSkipTablePredicate extends SkipTablePredicate {
        private final String conditionExpression;
        private final List<String> columnList;

        public ConditionSkipTablePredicate(String conditionExpression, List<String> columnList) {
            this.conditionExpression = conditionExpression;
            this.columnList = Collections.unmodifiableList(columnList);
        }

        public List<String> getColumnList() {
            return columnList;
        }

        public String getConditionExpression() {
            return conditionExpression;
        }
    }

    private static class LogicalOr implements BiPredicate<String, String> {
        private final BiPredicate<String, String>[] predicates;

        private LogicalOr(BiPredicate<String, String>... predicates) {
            this.predicates = predicates;
        }

        @Override
        public boolean test(String s, String s2) {
            for (BiPredicate<String, String> biPredicate : predicates) {
                if (biPredicate != null && biPredicate.test(s, s2)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static String[] filterEmpty(String[] strings) {
        List<String> list = new ArrayList<>();
        for (String string : strings) {
            if (string == null || string.trim().isEmpty()) {
                continue;
            }
            list.add(string);
        }
        return list.toArray(new String[0]);
    }

}
