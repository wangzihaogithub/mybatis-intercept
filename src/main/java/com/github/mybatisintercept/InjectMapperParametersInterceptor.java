package com.github.mybatisintercept;

import com.github.mybatisintercept.util.BeanMap;
import com.github.mybatisintercept.util.MybatisUtil;
import com.github.mybatisintercept.util.PlatformDependentUtil;
import com.github.mybatisintercept.util.StaticMethodAccessor;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

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
    private final Set<String> unmodifiableAttrNames = Collections.unmodifiableSet(attrNames);
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

        InterceptContext interceptContext = new InterceptContext(invocation, this);
        MybatisUtil.setParameterValue(invocation, metaName, new MetaMap(interceptContext));
        return invocation.proceed();
    }

    public boolean isGetterMetaProperty(String propertyName) {
        return propertyName != null && propertyName.startsWith(metaName + ".");
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

    private Properties getProperties() {
        Properties result = this.properties;
        if (result == null || result.isEmpty()) {
            result = System.getProperties();
        }
        if (PlatformDependentUtil.SPRING_ENVIRONMENT_READY) {
            result = PlatformDependentUtil.resolveSpringPlaceholders(result, "InjectMapperParametersInterceptor.");
        }
        return result;
    }

    public void initIfNeed() {
        if (!initFlag.compareAndSet(false, true)) {
            return;
        }
        Properties properties = getProperties();
        String valueProviderString = properties.getProperty("InjectMapperParametersInterceptor.valueProvider", "com.github.securityfilter.util.AccessUserUtil#getAccessUserValue");
        String metaNameString = properties.getProperty("InjectMapperParametersInterceptor.metaName", "_meta");
        String attrNamesString = properties.getProperty("InjectMapperParametersInterceptor.attrNames", "id,tenantId");

        this.valueProvider = new StaticMethodAccessor<>(valueProviderString);
        this.metaName = metaNameString;
        if (attrNamesString.trim().length() > 0) {
            try {
                this.attrNames.addAll(parseAttrNames(attrNamesString));
            } catch (ClassNotFoundException e) {
                PlatformDependentUtil.sneakyThrows(e);
            }
        }
    }

    protected Set<String> parseAttrNames(String attrNamesString) throws ClassNotFoundException {
        List<String> attrNamesList = Arrays.stream(attrNamesString.trim().split(",")).map(String::trim).distinct().collect(Collectors.toList());
        Set<String> set = new LinkedHashSet<>(attrNamesList.size());
        for (String s : attrNamesList) {
            Set<String> includeClassAttrNames = parseIncludeClassAttrNames(s);
            if (includeClassAttrNames != null) {
                set.addAll(includeClassAttrNames);
            } else {
                set.add(s);
            }
        }
        return set;
    }

    protected Set<String> parseIncludeClassAttrNames(String attrName) throws ClassNotFoundException {
        if (attrName.contains(".") || attrName.contains("/")) {
            Class<?> aClass = Class.forName(attrName.replace("/", "."), false, InjectMapperParametersInterceptor.class.getClassLoader());
            return BeanMap.findPropertyDescriptor(aClass).keySet();
        } else {
            return null;
        }
    }

    public static class InterceptContext implements com.github.mybatisintercept.InterceptContext<InjectMapperParametersInterceptor> {
        private final Invocation invocation;
        private final InjectMapperParametersInterceptor interceptor;
        private Map<String, Object> attributeMap;
        private Map<String, Object> meta;

        public InterceptContext(Invocation invocation, InjectMapperParametersInterceptor interceptor) {
            this.invocation = invocation;
            this.interceptor = interceptor;
        }

        @Override
        public InjectMapperParametersInterceptor getInterceptor() {
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
        public StaticMethodAccessor<com.github.mybatisintercept.InterceptContext<InjectMapperParametersInterceptor>> getValueProvider() {
            return (StaticMethodAccessor) interceptor.valueProvider;
        }

        public Map<String, Object> getMeta() {
            if (meta == null) {
                Map<String, Object> map = new LinkedHashMap<>((int) (interceptor.attrNames.size() / 0.75F) + 1);
                for (String attrName : interceptor.attrNames) {
                    map.put(attrName, interceptor.valueProvider.invokeWithOnBindContext(attrName, this));
                }
                this.meta = map;
            }
            return meta;
        }
    }


    public static class MetaMap implements Map<String, Object> {
        private final InterceptContext context;

        private MetaMap(InterceptContext context) {
            this.context = context;
        }

        @Override
        public int size() {
            Map<String, Object> meta = context.meta;
            if (meta == null) {
                return context.interceptor.attrNames.size();
            } else {
                return meta.size();
            }
        }

        @Override
        public boolean isEmpty() {
            Map<String, Object> meta = context.meta;
            if (meta == null) {
                return context.interceptor.attrNames.isEmpty();
            } else {
                return meta.isEmpty();
            }
        }

        @Override
        public boolean containsKey(Object key) {
            Map<String, Object> meta = context.meta;
            if (meta == null) {
                return context.interceptor.attrNames.contains(key);
            } else {
                return meta.containsKey(key);
            }
        }

        @Override
        public boolean containsValue(Object value) {
            return context.getMeta().containsValue(value);
        }

        @Override
        public Object get(Object key) {
            if (!(key instanceof String)) {
                return null;
            }
            Map<String, Object> meta = context.meta;
            if (meta == null) {
                return context.interceptor.valueProvider.invokeWithOnBindContext((String) key, context);
            } else {
                return meta.get(key);
            }
        }

        @Override
        public Object put(String key, Object value) {
            return context.getMeta().put(key, value);
        }

        @Override
        public Object remove(Object key) {
            return context.getMeta().remove(key);
        }

        @Override
        public void putAll(Map<? extends String, ?> m) {
            context.getMeta().putAll(m);
        }

        @Override
        public void clear() {
            context.getMeta().clear();
        }

        @Override
        public Set<String> keySet() {
            Map<String, Object> meta = context.meta;
            if (meta == null) {
                return context.interceptor.unmodifiableAttrNames;
            } else {
                return meta.keySet();
            }
        }

        @Override
        public Collection<Object> values() {
            return context.getMeta().values();
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return context.getMeta().entrySet();
        }
    }
}
