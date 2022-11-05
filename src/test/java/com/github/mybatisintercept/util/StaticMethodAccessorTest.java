package com.github.mybatisintercept.util;

import org.junit.Assert;
import org.junit.Test;

public class StaticMethodAccessorTest {
    private final StaticMethodAccessor<Void> getValueMethodAccessor = new StaticMethodAccessor<>(
            "com.github.mybatisintercept.util.StaticMethodAccessorTest#getValue");

    private final StaticMethodAccessor<MyContext> getValueWithOnBindContextMethodAccessor = new StaticMethodAccessor<>(
            "com.github.mybatisintercept.util.StaticMethodAccessorTest#getValueWithOnBindContext");

    public static void main(String[] args) {
        StaticMethodAccessorTest test = new StaticMethodAccessorTest();
        test.invoke();
        test.invokeWithOnBindContext();
    }

    @Test
    public void invokeWithOnBindContext() {
        MyContext context = new MyContext();
        Object userId = getValueWithOnBindContextMethodAccessor.invokeWithOnBindContext("userId", context);
    }

    @Test
    public void invoke() {
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
