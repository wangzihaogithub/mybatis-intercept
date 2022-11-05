package com.github.mybatisintercept;

import com.github.mybatisintercept.util.*;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;

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
    private final Set<String> interceptPackageNames = new LinkedHashSet<>();
    private final Set<String> skipTableNames = new LinkedHashSet<>();
    private final AtomicBoolean initFlag = new AtomicBoolean();
    private String dbType;
    private ASTDruidConditionUtil.ExistInjectConditionStrategyEnum existInjectConditionStrategyEnum;
    private SQL conditionExpression;
    private StaticMethodAccessor<InterceptContext> valueProvider;
    private BiPredicate<String, String> skipPredicate = (schema, tableName) -> {
        return skipTableNames.contains(tableName);
    };

    private void initIfNeed() {
        // Spring bean 方式配置时，如果没有配置属性就不会执行下面的 setProperties 方法，就不会初始化 因此这里会出现 null 的情况
        if (initFlag.compareAndSet(false, true)) {
            setProperties(System.getProperties());
        }
    }

    public static InterceptContext getInterceptContext() {
        return StaticMethodAccessor.getContext(InterceptContext.class);
    }

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        initIfNeed();

        InterceptContext interceptContext = new InterceptContext(invocation, this);
        if (isSupportIntercept(interceptContext)) {
            String rawSql = MybatisUtil.getBoundSqlString(invocation);
            String injectCondition = compileInject(interceptContext);
            String newSql = ASTDruidUtil.addAndCondition(rawSql, injectCondition, existInjectConditionStrategyEnum, dbType, skipPredicate);
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
        if (properties == null || properties.isEmpty()) {
            properties = System.getProperties();
        }
        String valueProvider = properties.getProperty("InjectConditionSQLInterceptor.valueProvider", "com.github.securityfilter.util.AccessUserUtil#getAccessUserValue");
        String dbType = properties.getProperty("InjectConditionSQLInterceptor.dbType", "mysql");
        String existInjectConditionStrategyEnum = properties.getProperty("InjectConditionSQLInterceptor.existInjectConditionStrategyEnum", "RULE_TABLE_MATCH_THEN_SKIP_SQL");

        String conditionExpression = properties.getProperty("InjectConditionSQLInterceptor.conditionExpression", "tenant_id = ${tenantId}"); // 字符串请这样写： 字段 = '${属性}'
        String interceptPackageNames = properties.getProperty("InjectConditionSQLInterceptor.interceptPackageNames", ""); // 空字符=不限制，全拦截
        String skipTableNames = properties.getProperty("InjectConditionSQLInterceptor.skipTableNames", "");

        this.valueProvider = new StaticMethodAccessor<>(valueProvider);
        this.dbType = dbType;
        this.existInjectConditionStrategyEnum = ASTDruidConditionUtil.ExistInjectConditionStrategyEnum.valueOf(existInjectConditionStrategyEnum);
        this.conditionExpression = SQL.compile(conditionExpression, Collections.emptyMap());
        if (interceptPackageNames.trim().length() > 0) {
            this.interceptPackageNames.addAll(Arrays.asList(interceptPackageNames.trim().split(",")));
        }
        if (skipTableNames.trim().length() > 0) {
            this.skipTableNames.addAll(Arrays.asList(skipTableNames.trim().split(",")));
        }
    }

    public static class InterceptContext {
        private final Invocation invocation;
        private final InjectConditionSQLInterceptor interceptor;

        public InterceptContext(Invocation invocation, InjectConditionSQLInterceptor interceptor) {
            this.invocation = invocation;
            this.interceptor = interceptor;
        }

        public InjectConditionSQLInterceptor getInterceptor() {
            return interceptor;
        }

        public Invocation getInvocation() {
            return invocation;
        }

        public String getBoundSqlString() {
            return MybatisUtil.getBoundSqlString(invocation);
        }

        public Method getMapperMethod() {
            return MybatisUtil.getMapperMethod(invocation);
        }

        public Class<?> getMapperClass() {
            return MybatisUtil.getMapperClass(MybatisUtil.getMappedStatement(invocation));
        }
    }

}
