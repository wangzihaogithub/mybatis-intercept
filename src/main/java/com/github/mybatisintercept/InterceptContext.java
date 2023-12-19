package com.github.mybatisintercept;

import com.github.mybatisintercept.util.BeanMap;
import com.github.mybatisintercept.util.MybatisUtil;
import com.github.mybatisintercept.util.StaticMethodAccessor;
import com.github.mybatisintercept.util.TypeUtil;
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

    default Method getMapperMethod() {
        return MybatisUtil.getMapperMethod(getInvocation());
    }

    default Class<?> getMapperClass() {
        return MybatisUtil.getMapperClass(MybatisUtil.getMappedStatement(getInvocation()));
    }

    Map<String, Object> getAttributeMap();

    Object getAttributeValue(String name);

    StaticMethodAccessor<InterceptContext<INTERCEPTOR>> getValueProvider();

    interface ValueGetter {
        <T> T getValue(String name, Class<T> type);

        <T> T getProviderValue(String name, Class<T> type);

        <T> T getMybatisParameterValue(String name, Class<T> type);

        <T> T getInterceptAttributeValue(String name, Class<T> type);
    }

    default ValueGetter getValueGetter() {
        return new ValueGetter() {
            private Map<String, Object> parameter;

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
                if (parameter == null) {
                    parameter = BeanMap.toMap(getParameter());
                }
                Object value = parameter.get(name);
                return cast(value, type);
            }

            @Override
            public <T> T getInterceptAttributeValue(String name, Class<T> type) {
                Object value = getAttributeValue(name);
                return cast(value, type);
            }

            @Override
            public <T> T getValue(String name, Class<T> type) {
                Object value = getProviderValue(name, type);
                if (value == null) {
                    value = getMybatisParameterValue(name, type);
                }
                if (value == null) {
                    value = getInterceptAttributeValue(name, type);
                }
                return cast(value, type);
            }
        };
    }
}
