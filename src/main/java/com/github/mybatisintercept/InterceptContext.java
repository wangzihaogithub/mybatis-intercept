package com.github.mybatisintercept;

import com.github.mybatisintercept.util.MybatisUtil;
import com.github.mybatisintercept.util.StaticMethodAccessor;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Invocation;

import java.lang.reflect.Method;

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
}
