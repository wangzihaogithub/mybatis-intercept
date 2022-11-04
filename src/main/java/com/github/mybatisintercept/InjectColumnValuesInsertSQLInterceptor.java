package com.github.mybatisintercept;

import com.github.mybatisintercept.util.ASTDruidUtil;
import com.github.mybatisintercept.util.BeanMap;
import com.github.mybatisintercept.util.MybatisUtil;
import com.github.mybatisintercept.util.StaticMethodAccessor;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.plugin.*;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 自动给（insert语句, replace语句）加字段拦截器
 *
 * @author wangzihaogithub 2022-11-04
 */
@Intercepts({@Signature(type = Executor.class, method = "update", args = {MappedStatement.class, Object.class})})
public class InjectColumnValuesInsertSQLInterceptor implements Interceptor {
    private final Set<String> interceptPackageNames = new LinkedHashSet<>();
    private final AtomicBoolean initFlag = new AtomicBoolean();
    private StaticMethodAccessor<InterceptContext> valueProvider;
    private Set<ColumnMapping> columnMappings;
    private String dbType;

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

            // 找sql里的预编译问号
            int columnParameterIndex = ASTDruidUtil.getColumnParameterIndex(newSql, columnName, dbType);
            if (columnParameterIndex == -1) {
                // 1. 用户没有填写column字段。 insert into x_table (`id`, `name`, `这里没有填写column字段`)
                newSql = ASTDruidUtil.addColumnValues(rawSql, columnName, columnValue, dbType);
            } else {
                // 2. 用户主动填写了column字段。 insert into x_table (`id`, `name`, `这里主动填写column字段`)
                BoundSql boundSql = MybatisUtil.getBoundSql(interceptContext.invocation);
                List<ParameterMapping> parameterMappingList = boundSql.getParameterMappings();
                ParameterMapping parameterMapping = parameterMappingList.get(columnParameterIndex);
                Object parameterObject = boundSql.getParameterObject();

                // 向实体类里自动回填属性值
                boolean setterSuccess = invokeParameterObjectSetter(parameterObject, parameterMapping, columnValue);
                if (!setterSuccess) {
                    // 用户实体类里没有这个属性，删掉拼接的?参数, 改sql，将字段写为常量至values里
                    parameterMappingList.remove(columnParameterIndex);
                    newSql = ASTDruidUtil.addColumnValues(rawSql, columnName, columnValue, dbType);
                }
            }
        }
        return newSql;
    }

    protected boolean invokeParameterObjectSetter(Object parameterObject, ParameterMapping parameterMapping, Object value) {
        String property = parameterMapping.getProperty();
        Map beanHandler;
        if (parameterObject instanceof Map) {
            beanHandler = (Map) parameterObject;
        } else {
            beanHandler = new BeanMap(parameterObject);
            // 用户实体类里没有这个属性
            if (!beanHandler.containsKey(property)) {
                return false;
            }
        }

        Object existValue = beanHandler.get(property);
        if (existValue != null && !"".equals(existValue)) {
            // 用户自己赋值了, 不更改用户填的值
        } else {
            // 用户没有赋值，自动回填至实体类
            beanHandler.put(property, value);
        }
        return true;
    }

    protected boolean isSupportIntercept(InterceptContext interceptContext) {
        return MybatisUtil.isInterceptPackage(interceptContext.invocation, interceptPackageNames)
                && existColumnValue(interceptContext)
                && ASTDruidUtil.isInsertOrReplace(MybatisUtil.getBoundSqlString(interceptContext.invocation), dbType);
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
        if (properties == null || properties.isEmpty()) {
            properties = System.getProperties();
        }
        String valueProvider = properties.getProperty("InjectColumnValuesInsertSQLInterceptor.valueProvider", "com.github.securityfilter.util.AccessUserUtil#getAccessUserValue");
        String dbType = properties.getProperty("InjectColumnValuesInsertSQLInterceptor.dbType", "mysql");
        String columnMappings = properties.getProperty("InjectColumnValuesInsertSQLInterceptor.columnMappings", "tenant_id=tenantId"); // tenant_id=tenantId,u_id=uId
        String interceptPackageNames = properties.getProperty("InjectColumnValuesInsertSQLInterceptor.interceptPackageNames", ""); // 空字符=不限制，全拦截

        this.valueProvider = new StaticMethodAccessor<>(valueProvider);
        this.dbType = dbType;
        this.columnMappings = ColumnMapping.parse(columnMappings);
        if (interceptPackageNames.trim().length() > 0) {
            this.interceptPackageNames.addAll(Arrays.asList(interceptPackageNames.trim().split(",")));
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
        private final InjectColumnValuesInsertSQLInterceptor interceptor;

        public InterceptContext(Invocation invocation, InjectColumnValuesInsertSQLInterceptor interceptor) {
            this.invocation = invocation;
            this.interceptor = interceptor;
        }

        public InjectColumnValuesInsertSQLInterceptor getInterceptor() {
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
