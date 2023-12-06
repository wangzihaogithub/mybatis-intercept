package com.github.mybatisintercept.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.function.Function;

public class StaticMethodAccessor<CONTEXT> implements Function<String, Object> {
    private final String classMethodName;
    private final Method method;
    private static final ThreadLocal<Object> CONTEXT_THREAD_LOCAL = new ThreadLocal<>();

    public StaticMethodAccessor(String classMethodName) {
        Method method = null;
        try {
            String[] split = classMethodName.split("#");
            Class<?> clazz = Class.forName(split[0]);
            method = clazz.getDeclaredMethod(split[1], String.class);
            method.setAccessible(true);
        } catch (Exception e) {
            PlatformDependentUtil.sneakyThrows(e);
        }
        if (!Modifier.isStatic(method.getModifiers())) {
            throw new IllegalArgumentException("must is static method. " + classMethodName);
        }
        this.classMethodName = classMethodName;
        this.method = method;
    }

    public static <T> T getContext(Class<T> type) {
        Object context = CONTEXT_THREAD_LOCAL.get();
        if (context != null && type.isAssignableFrom(context.getClass())) {
            return (T) context;
        } else {
            return null;
        }
    }

    public Object invokeWithOnBindContext(String name, CONTEXT context) {
        try {
            CONTEXT_THREAD_LOCAL.set(context);
            return apply(name);
        } finally {
            CONTEXT_THREAD_LOCAL.remove();
        }
    }

    @Override
    public Object apply(String name) {
        try {
            return method.invoke(null, name);
        } catch (IllegalAccessException | InvocationTargetException e) {
            PlatformDependentUtil.sneakyThrows(e);
            return null;
        }
    }

    @Override
    public String toString() {
        return classMethodName;
    }

}