package com.github.mybatisintercept;

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class InjectConditionSQLInterceptorTest {
    public static void main(String[] args) {
        InjectConditionSQLInterceptorTest test = new InjectConditionSQLInterceptorTest();

        test.setProperties();
        test.openSelector();
        System.out.println();
    }

    @Test
    public void setProperties() {
        InjectConditionSQLInterceptor interceptor = new InjectConditionSQLInterceptor();
        Properties properties = new Properties();
        properties.setProperty("InjectConditionSQLInterceptor.valueProvider", "com.github.mybatisintercept.InjectConditionSQLInterceptorTest#get");

        interceptor.setProperties(properties);
    }

    @Test
    public void openSelector() {
        InjectConditionSQLInterceptor.setSelector(1, 1);
        try (InjectSelector injectSelector = InjectConditionSQLInterceptor.openSelector()) {
            Assert.assertArrayEquals(new Integer[]{1, 1}, injectSelector.begin(2, 2));
            Assert.assertArrayEquals(new Integer[]{2, 2}, InjectConditionSQLInterceptor.getSelector());
            Assert.assertArrayEquals(new Integer[]{1, 1}, injectSelector.end());
            Assert.assertArrayEquals(new Integer[]{1, 1}, InjectConditionSQLInterceptor.getSelector());

            Assert.assertArrayEquals(new Integer[]{1, 1}, injectSelector.begin(3, 3));
            Assert.assertArrayEquals(new Integer[]{3, 3}, InjectConditionSQLInterceptor.getSelector());
        }

        Assert.assertArrayEquals(new Integer[]{1, 1}, InjectConditionSQLInterceptor.getSelector());
    }

    public static Object get(String name) {
        return null;
    }
}
