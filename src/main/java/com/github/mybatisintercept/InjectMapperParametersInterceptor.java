package com.github.mybatisintercept;

import com.github.mybatisintercept.util.MybatisUtil;
import com.github.mybatisintercept.util.PlatformDependentUtil;
import com.github.mybatisintercept.util.StaticMethodAccessor;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * 给 mapper.xml 加全局内置属性（可以在 mapper.xml 里直接访问这些属性）
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
public class InjectMapperParametersInterceptor implements Interceptor {
    private final AtomicBoolean initFlag = new AtomicBoolean();
    private final Set<String> attrNames = new LinkedHashSet<>();
    private StaticMethodAccessor<InterceptContext> valueProvider;
    private String metaName;
    private Properties properties;

    public static InterceptContext getInterceptContext() {
        return StaticMethodAccessor.getContext(InterceptContext.class);
    }

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        // Spring bean 方式配置时，如果没有配置属性就不会执行下面的 setProperties 方法，就不会初始化 因此这里会出现 null 的情况
        initIfNeed();

        if (isSupportIntercept(invocation)) {
            InterceptContext interceptContext = new InterceptContext(invocation, this);
            MybatisUtil.setParameterValue(invocation, metaName, getMeta(interceptContext));
        }
        return invocation.proceed();
    }

    public boolean isGetterMetaProperty(String propertyName) {
        return propertyName != null && propertyName.startsWith(metaName + ".");
    }

    protected boolean isSupportIntercept(Invocation invocation) {
        List<ParameterMapping> parameterMappingList = MybatisUtil.getBoundSql(invocation).getParameterMappings();
        if (parameterMappingList != null) {
            for (ParameterMapping parameterMapping : parameterMappingList) {
                if (isGetterMetaProperty(parameterMapping.getProperty())) {
                    return true;
                }
            }
        }
        return false;
    }

    protected Object getMeta(InterceptContext interceptContext) {
        Map<String, Object> user = new LinkedHashMap<>((int) (attrNames.size() / 2 * 0.75));
        for (String attrName : attrNames) {
            user.put(attrName, valueProvider.invokeWithOnBindContext(attrName, interceptContext));
        }
        return user;
    }

    public StaticMethodAccessor<InterceptContext> getValueProvider() {
        return valueProvider;
    }

    public Set<String> getAttrNames() {
        return attrNames;
    }

    public String getMetaName() {
        return metaName;
    }

    public void setValueProvider(StaticMethodAccessor<InterceptContext> valueProvider) {
        this.valueProvider = valueProvider;
    }

    public void setMetaName(String metaName) {
        this.metaName = metaName;
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
            properties = PlatformDependentUtil.resolveSpringPlaceholders(properties, "InjectMapperParametersInterceptor.");
        }
        String valueProvider = properties.getProperty("InjectMapperParametersInterceptor.valueProvider", "com.github.securityfilter.util.AccessUserUtil#getAccessUserValue");
        String metaName = properties.getProperty("InjectMapperParametersInterceptor.metaName", "_meta");
        String attrNames = properties.getProperty("InjectMapperParametersInterceptor.attrNames", "id,tenantId");

        this.valueProvider = new StaticMethodAccessor<>(valueProvider);
        this.metaName = metaName;
        if (attrNames.trim().length() > 0) {
            this.attrNames.addAll(Arrays.stream(attrNames.trim().split(",")).map(String::trim).collect(Collectors.toList()));
        }
    }

    public static class InterceptContext {
        private final Invocation invocation;
        private final InjectMapperParametersInterceptor interceptor;

        public InterceptContext(Invocation invocation, InjectMapperParametersInterceptor interceptor) {
            this.invocation = invocation;
            this.interceptor = interceptor;
        }

        public InjectMapperParametersInterceptor getInterceptor() {
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
