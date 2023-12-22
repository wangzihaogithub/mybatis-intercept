package com.github.mybatisintercept;

import com.github.mybatisintercept.util.BeanMap;
import com.github.mybatisintercept.util.MybatisUtil;
import com.github.mybatisintercept.util.TypeUtil;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.plugin.Interceptor;

import java.util.Map;

public class ValueGetterImpl implements InterceptContext.ValueGetter {
    private final Object mybatisParameter;
    private final Map<String, Object> mybatisParameterGetter;
    private final InterceptContext<Interceptor> interceptContext;

    public ValueGetterImpl(InterceptContext interceptContext) {
        this.interceptContext = interceptContext;
        this.mybatisParameter = interceptContext.getParameter();
        this.mybatisParameterGetter = MybatisUtil.isInstanceofKeyValue(mybatisParameter) ? BeanMap.toMap(mybatisParameter) : null;
    }

    protected <T> T cast(Object value, Class<T> type) {
        return type == null || type == Object.class ? (T) value : TypeUtil.cast(value, type);
    }

    @Override
    public <T> T getProviderValue(String name, Class<T> type) {
        Object value = interceptContext.getValueProvider().invokeWithOnBindContext(name, interceptContext);
        return cast(value, type);
    }

    @Override
    public <T> T getMybatisParameterValue(String name, Class<T> type) {
        Object value = mybatisParameterGetter != null && mybatisParameterGetter.containsKey(name) ? mybatisParameterGetter.get(name) : null;
        return cast(value, type);
    }

    @Override
    public <T> T getMybatisBoundSqlValue(String name, Class<T> type) {
        BoundSql boundSql = MybatisUtil.getBoundSql(interceptContext.getInvocation());
        Object value = boundSql.hasAdditionalParameter(name) ? boundSql.getAdditionalParameter(name) : null;
        return cast(value, type);
    }

    @Override
    public <T> T getInterceptAttributeValue(String name, Class<T> type) {
        Object value = interceptContext.getAttributeValue(name);
        return cast(value, type);
    }

    @Override
    public Object getCompileValue(String name) {
        Object value = getProviderValue(name, null);
        if (value == null) {
            value = getInterceptAttributeValue(name, null);
        }
        return value;
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
}
