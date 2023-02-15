package com.github.mybatisintercept;

import com.github.mybatisintercept.util.ASTDruidUtil;
import com.github.mybatisintercept.util.MybatisUtil;
import com.github.mybatisintercept.util.PlatformDependentUtil;
import com.github.mybatisintercept.util.StaticMethodAccessor;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.*;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * 自动给（update语句）加字段属性值， 如果值为空
 *
 * @author wangzihaogithub 2022-11-04
 */
@Intercepts({@Signature(type = Executor.class, method = "update", args = {MappedStatement.class, Object.class})})
public class InjectColumnValuesUpdateSQLInterceptor implements Interceptor {
    private final Set<String> interceptPackageNames = new LinkedHashSet<>();
    private final AtomicBoolean initFlag = new AtomicBoolean();
    private final Set<String> skipTableNames = new LinkedHashSet<>();
    private StaticMethodAccessor<InterceptContext> valueProvider;
    private Set<ColumnMapping> columnMappings;
    private String dbType;
    private BiPredicate<String, String> skipPredicate = (schema, tableName) -> {
        return skipTableNames.contains(tableName);
    };
    private Properties properties;

    public static InterceptContext getInterceptContext() {
        return StaticMethodAccessor.getContext(InterceptContext.class);
    }

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        // Spring bean 方式配置时，如果没有配置属性就不会执行下面的 setProperties 方法，就不会初始化 因此这里会出现 null 的情况
        initIfNeed();

        InterceptContext interceptContext = new InterceptContext(invocation, this);
        if (isSupportIntercept(interceptContext)) {
            addColumnValues(interceptContext);
        }
        return invocation.proceed();
    }

    protected void addColumnValues(InterceptContext interceptContext) {
        BoundSql boundSql = MybatisUtil.getBoundSql(interceptContext.invocation);
        for (ColumnMapping columnMapping : columnMappings) {
            Object columnValue = valueProvider.invokeWithOnBindContext(columnMapping.getAttrName(), interceptContext);
            MybatisUtil.invokeParameterObjectSetter(boundSql, columnMapping.getProperty(), columnValue);
        }
    }

    protected boolean isSupportIntercept(InterceptContext interceptContext) {
        return MybatisUtil.isInterceptPackage(interceptContext.invocation, interceptPackageNames)
                && existColumnValue(interceptContext)
                && ASTDruidUtil.isNoSkipUpdate(MybatisUtil.getBoundSqlString(interceptContext.invocation), dbType, skipPredicate);
    }

    protected boolean existColumnValue(InterceptContext interceptContext) {
        for (ColumnMapping item : columnMappings) {
            Object value = valueProvider.invokeWithOnBindContext(item.getAttrName(), interceptContext);
            if (value == null) {
                return false;
            }
        }
        return true;
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
            properties = PlatformDependentUtil.resolveSpringPlaceholders(properties, "InjectColumnValuesUpdateSQLInterceptor.");
        }
        String valueProvider = properties.getProperty("InjectColumnValuesUpdateSQLInterceptor.valueProvider", "com.github.securityfilter.util.AccessUserUtil#getAccessUserValue");
        String dbType = properties.getProperty("InjectColumnValuesUpdateSQLInterceptor.dbType", "mysql");
        String columnMappings = properties.getProperty("InjectColumnValuesUpdateSQLInterceptor.columnMappings", "tenantId=tenantId"); // tenantId=tenantId,tenantId
        String interceptPackageNames = properties.getProperty("InjectColumnValuesUpdateSQLInterceptor.interceptPackageNames", ""); // 空字符=不限制，全拦截
        String skipTableNames = properties.getProperty("InjectColumnValuesUpdateSQLInterceptor.skipTableNames", "");

        this.valueProvider = new StaticMethodAccessor<>(valueProvider);
        this.dbType = dbType;
        this.columnMappings = ColumnMapping.parse(columnMappings);
        if (interceptPackageNames.trim().length() > 0) {
            this.interceptPackageNames.addAll(Arrays.asList(interceptPackageNames.trim().split(",")));
        }
        if (skipTableNames.trim().length() > 0) {
            this.skipTableNames.addAll(Arrays.stream(skipTableNames.trim().split(",")).map(String::trim).collect(Collectors.toList()));
        }
    }

    public Set<String> getInterceptPackageNames() {
        return interceptPackageNames;
    }

    public Set<ColumnMapping> getColumnMappings() {
        return columnMappings;
    }

    public StaticMethodAccessor<InterceptContext> getValueProvider() {
        return valueProvider;
    }

    public String getDbType() {
        return dbType;
    }

    public void setColumnMappings(Set<ColumnMapping> columnMappings) {
        this.columnMappings = columnMappings;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public void setValueProvider(StaticMethodAccessor<InterceptContext> valueProvider) {
        this.valueProvider = valueProvider;
    }

    public BiPredicate<String, String> getSkipPredicate() {
        return skipPredicate;
    }

    public Set<String> getSkipTableNames() {
        return skipTableNames;
    }

    public void setSkipPredicate(BiPredicate<String, String> skipPredicate) {
        this.skipPredicate = Objects.requireNonNull(skipPredicate);
    }

    public static class ColumnMapping {
        private final String attrName;
        private final String property;

        public ColumnMapping(String attrName, String property) {
            this.attrName = attrName;
            this.property = property;
        }

        public String getAttrName() {
            return attrName;
        }

        public String getProperty() {
            return property;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ColumnMapping)) {
                return false;
            }
            ColumnMapping that = (ColumnMapping) o;
            return Objects.equals(property, that.property) && Objects.equals(attrName, that.attrName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(property, attrName);
        }

        @Override
        public String toString() {
            return attrName + "=" + property;
        }

        public static Set<ColumnMapping> parse(String str) {
            Set<ColumnMapping> list = new LinkedHashSet<>();
            for (String s : str.split(",")) {
                ColumnMapping bean;
                String[] split = s.split("=", 2);
                if (split.length == 1) {
                    bean = new ColumnMapping(split[0], split[0]);
                } else {
                    bean = new ColumnMapping(split[0], split[1]);
                }
                list.add(bean);
            }
            return list;
        }
    }

    public static class InterceptContext {
        private final Invocation invocation;
        private final InjectColumnValuesUpdateSQLInterceptor interceptor;

        public InterceptContext(Invocation invocation, InjectColumnValuesUpdateSQLInterceptor interceptor) {
            this.invocation = invocation;
            this.interceptor = interceptor;
        }

        public InjectColumnValuesUpdateSQLInterceptor getInterceptor() {
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
