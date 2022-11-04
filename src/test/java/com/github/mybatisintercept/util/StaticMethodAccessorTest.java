package com.github.mybatisintercept.util;

import org.junit.Assert;

public class StaticMethodAccessorTest {
    public static final StaticMethodAccessor<Void> getValueMethodAccessor = new StaticMethodAccessor<>(
            "com.github.mybatisintercept.util.StaticMethodAccessorTest#getValue");

    public static final StaticMethodAccessor<MyContext> getValueWithOnBindContextMethodAccessor = new StaticMethodAccessor<>(
            "com.github.mybatisintercept.util.StaticMethodAccessorTest#getValueWithOnBindContext");

    public static void main(String[] args) {
        invoke();
        invokeWithOnBindContext();
    }

    public static void invokeWithOnBindContext() {
        MyContext context = new MyContext();
        Object userId = getValueWithOnBindContextMethodAccessor.invokeWithOnBindContext("userId", context);
    }

    public static void invoke() {
        Object userId1 = getValue("userId");
        Object userId2 = getValueMethodAccessor.apply("userId");

        Assert.assertEquals(userId1, userId2);
    }

    public static Object getValueWithOnBindContext(String name) {
        MyContext context = StaticMethodAccessor.getContext(MyContext.class);
        Assert.assertNotNull(context);
        return name.hashCode();
    }

    public static Object getValue(String name) {
        return name.hashCode();
    }

    public static class MyContext {

    }
}
