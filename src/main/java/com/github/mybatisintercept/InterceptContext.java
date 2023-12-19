package com.github.mybatisintercept;

import com.github.mybatisintercept.util.BeanMap;
import com.github.mybatisintercept.util.MybatisUtil;
import com.github.mybatisintercept.util.StaticMethodAccessor;
import com.github.mybatisintercept.util.TypeUtil;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Invocation;

import java.lang.reflect.Method;
import java.util.Map;

public interface InterceptContext<INTERCEPTOR extends Interceptor> {

    static InterceptContext<Interceptor> getContext() {
        return StaticMethodAccessor.getContext(InterceptContext.class);
    }

    INTERCEPTOR getInterceptor();

    Invocation getInvocation();

    default String getBoundSqlString() {
        return MybatisUtil.getBoundSqlString(getInvocation());
    }

    default Object getParameter() {
        return MybatisUtil.getParameter(getInvocation());
    }

    default boolean isParameterInstanceofKeyValue() {
        return MybatisUtil.isInstanceofKeyValue(getParameter());
    }

    default Method getMapperMethod() {
        return MybatisUtil.getMapperMethod(getInvocation());
    }

    default Class<?> getMapperClass() {
        return MybatisUtil.getMapperClass(MybatisUtil.getMappedStatement(getInvocation()));
    }

    Map<String, Object> getAttributeMap();

    default Object putAttributeValue(String name, Object value) {
        return getAttributeMap().put(name, value);
    }

    Object getAttributeValue(String name);

    StaticMethodAccessor<InterceptContext<INTERCEPTOR>> getValueProvider();

    interface ValueGetter {
        Object getValue(String name);

        <T> T getValue(String name, Class<T> type);

        <T> T getProviderValue(String name, Class<T> type);

        <T> T getMybatisParameterValue(String name, Class<T> type);

        <T> T getMybatisBoundSqlValue(String name, Class<T> type);

        <T> T getInterceptAttributeValue(String name, Class<T> type);
    }

    default ValueGetter getValueGetter() {
        return new ValueGetter() {
            private final Object mybatisParameter = getParameter();
            private final Map<String, Object> mybatisParameterGetter = MybatisUtil.isInstanceofKeyValue(mybatisParameter) ? BeanMap.toMap(mybatisParameter) : null;

            private <T> T cast(Object value, Class<T> type) {
                return type == null || type == Object.class ? (T) value : TypeUtil.cast(value, type);
            }

            @Override
            public <T> T getProviderValue(String name, Class<T> type) {
                Object value = getValueProvider().invokeWithOnBindContext(name, InterceptContext.this);
                return cast(value, type);
            }

            @Override
            public <T> T getMybatisParameterValue(String name, Class<T> type) {
                Object value = mybatisParameterGetter != null && mybatisParameterGetter.containsKey(name) ? mybatisParameterGetter.get(name) : null;
                return cast(value, type);
            }

            @Override
            public <T> T getMybatisBoundSqlValue(String name, Class<T> type) {
                BoundSql boundSql = MybatisUtil.getBoundSql(getInvocation());
                Object value = boundSql.hasAdditionalParameter(name) ? boundSql.getAdditionalParameter(name) : null;
                return cast(value, type);
            }

            @Override
            public <T> T getInterceptAttributeValue(String name, Class<T> type) {
                Object value = getAttributeValue(name);
                return cast(value, type);
            }

            @Override
            public Object getValue(String name) {
                return getValue(name, null);
            }

            @Override
            public <T> T getValue(String name, Class<T> type) {
                Object value = getProviderValue(name, type);
                if (value == null) {
                    value = getMybatisParameterValue(name, type);
                }
                if (value == null) {
                    value = getMybatisBoundSqlValue(name, type);
                }
                if (value == null) {
                    value = getInterceptAttributeValue(name, type);
                }
                return cast(value, type);
            }
        };
    }
}
