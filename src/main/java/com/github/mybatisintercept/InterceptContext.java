package com.github.mybatisintercept;

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

    default <T> T getAttributeValue(String name, Class<T> type) {
        return TypeUtil.cast(getAttributeValue(name), type);
    }

    StaticMethodAccessor<InterceptContext<INTERCEPTOR>> getValueProvider();

    default ValueGetter getValueGetter() {
        return new ValueGetterImpl(this);
    }

    interface ValueGetter {
        Object getCompileValue(String name);

        <T> T getCompileValue(String name, Class<T> type);

        <T> T getProviderValue(String name, Class<T> type);

        <T> T getMybatisParameterValue(String name, Class<T> type);

        <T> T getMybatisBoundSqlValue(String name, Class<T> type);

        <T> T getInterceptAttributeValue(String name, Class<T> type);
    }

}
