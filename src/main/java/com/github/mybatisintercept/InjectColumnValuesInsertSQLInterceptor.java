package com.github.mybatisintercept;

import com.github.mybatisintercept.util.ASTDruidUtil;
import com.github.mybatisintercept.util.MybatisUtil;
import com.github.mybatisintercept.util.PlatformDependentUtil;
import com.github.mybatisintercept.util.StaticMethodAccessor;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.plugin.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * 自动给（insert语句, replace语句）加字段
 *
 * @author wangzihaogithub 2022-11-04
 */
@Intercepts({@Signature(type = Executor.class, method = "update", args = {MappedStatement.class, Object.class})})
public class InjectColumnValuesInsertSQLInterceptor implements Interceptor {
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
            String rawSql = MybatisUtil.getBoundSqlString(invocation);
            String newSql = addColumnValues(interceptContext, rawSql);
            if (!Objects.equals(rawSql, newSql)) {
                MybatisUtil.rewriteSql(invocation, newSql);
            }
        }
        return invocation.proceed();
    }

    protected String addColumnValues(InterceptContext interceptContext, String rawSql) {
        String newSql = rawSql;
        for (ColumnMapping columnMapping : columnMappings) {
            String columnName = columnMapping.getColumnName();
            Object columnValue = valueProvider.invokeWithOnBindContext(columnMapping.getAttrName(), interceptContext);

            // 找sql里的参数化预编译问号下标
            int columnParameterizedIndex = ASTDruidUtil.getColumnParameterizedIndex(newSql, columnName, dbType);
            if (columnParameterizedIndex == -1) {
                // 1. 用户没有填写column字段。给他加column。 对应： insert into x_table (`a`, `缺失`) values (?)
                newSql = ASTDruidUtil.addColumnValues(newSql, columnName, columnValue, dbType);
            } else if (columnParameterizedIndex == -2) {
                // 2. 用户主动填写了column字段, values是有值的常量. 不管他。对应：insert into x_table (`a`, `b`) values (?, 1)
            } else {
                // 3. 用户主动填写了column字段, values是参数化. 就给对象赋值，对应：insert into x_table (`a`, `b`) values (?, ?)
                BoundSql boundSql = MybatisUtil.getBoundSql(interceptContext.invocation);
                String property = MybatisUtil.getParameterMappingProperty(boundSql, columnParameterizedIndex);

                // 向实体类里自动回填属性值
                boolean setterSuccess = MybatisUtil.invokeParameterObjectSetter(boundSql, property, columnValue);
                if (!setterSuccess) {
                    // 用户实体类里没有这个属性，删掉拼接的?参数, 改sql，将字段写为常量至values里
                    List<ParameterMapping> removeParameterMappingList = MybatisUtil.removeParameterMapping(boundSql, property);
                    if (removeParameterMappingList.size() > 0) {
                        newSql = ASTDruidUtil.addColumnValues(newSql, columnName, columnValue, dbType);
                    }
                }
            }
        }
        return newSql;
    }

    protected boolean isSupportIntercept(InterceptContext interceptContext) {
        return MybatisUtil.isInterceptPackage(interceptContext.invocation, interceptPackageNames)
                && existColumnValue(interceptContext)
                && ASTDruidUtil.isNoSkipInsertOrReplace(MybatisUtil.getBoundSqlString(interceptContext.invocation), dbType, skipPredicate);
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
            properties = PlatformDependentUtil.resolveSpringPlaceholders(properties, "InjectColumnValuesInsertSQLInterceptor.");
        }
        String valueProvider = properties.getProperty("InjectColumnValuesInsertSQLInterceptor.valueProvider", "com.github.securityfilter.util.AccessUserUtil#getAccessUserValue");
        String dbType = properties.getProperty("InjectColumnValuesInsertSQLInterceptor.dbType", "mysql");
        String columnMappings = properties.getProperty("InjectColumnValuesInsertSQLInterceptor.columnMappings", "tenant_id=tenantId"); // tenant_id=tenantId,u_id=uId
        String interceptPackageNames = properties.getProperty("InjectColumnValuesInsertSQLInterceptor.interceptPackageNames", ""); // 空字符=不限制，全拦截
        String skipTableNames = properties.getProperty("InjectColumnValuesInsertSQLInterceptor.skipTableNames", "");

        this.valueProvider = new StaticMethodAccessor<>(valueProvider);
        this.dbType = dbType;
        this.columnMappings = ColumnMapping.parse(columnMappings);
        if (interceptPackageNames.trim().length() > 0) {
            this.interceptPackageNames.addAll(Arrays.stream(interceptPackageNames.trim().split(",")).map(String::trim).collect(Collectors.toList()));
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

    public Set<String> getSkipTableNames() {
        return skipTableNames;
    }

    public BiPredicate<String, String> getSkipPredicate() {
        return skipPredicate;
    }

    public void setSkipPredicate(BiPredicate<String, String> skipPredicate) {
        this.skipPredicate = skipPredicate;
    }

    public static class ColumnMapping {
        private final String columnName;
        private final String attrName;

        public ColumnMapping(String columnName, String attrName) {
            this.columnName = columnName;
            this.attrName = attrName;
        }

        public String getColumnName() {
            return columnName;
        }

        public String getAttrName() {
            return attrName;
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
            return Objects.equals(columnName, that.columnName) && Objects.equals(attrName, that.attrName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnName, attrName);
        }

        @Override
        public String toString() {
            return columnName + "=" + attrName;
        }

        public static Set<ColumnMapping> parse(String str) {
            Set<ColumnMapping> list = new LinkedHashSet<>();
            for (String s : str.split(",")) {
                s = s.trim();
                if (s.isEmpty()) {
                    continue;
                }

                ColumnMapping bean;
                String[] split = s.split("=", 2);
                if (split.length == 1) {
                    bean = new ColumnMapping(split[0].trim(), split[0].trim());
                } else {
                    bean = new ColumnMapping(split[0].trim(), split[1].trim());
                }
                list.add(bean);
            }
            return list;
        }
    }

    public static class InterceptContext implements com.github.mybatisintercept.InterceptContext<InjectColumnValuesInsertSQLInterceptor> {
        private final Invocation invocation;
        private final InjectColumnValuesInsertSQLInterceptor interceptor;

        public InterceptContext(Invocation invocation, InjectColumnValuesInsertSQLInterceptor interceptor) {
            this.invocation = invocation;
            this.interceptor = interceptor;
        }

        @Override
        public InjectColumnValuesInsertSQLInterceptor getInterceptor() {
            return interceptor;
        }

        @Override
        public Invocation getInvocation() {
            return invocation;
        }

    }
}
