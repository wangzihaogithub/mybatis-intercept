package com.github.mybatisintercept;

import org.junit.Test;

import java.util.Properties;

public class InjectConditionSQLInterceptorTest {
    public static void main(String[] args) {
        InjectConditionSQLInterceptorTest test = new InjectConditionSQLInterceptorTest();

        test.setProperties();
        System.out.println();
    }

    @Test
    public void setProperties() {
        InjectConditionSQLInterceptor interceptor = new InjectConditionSQLInterceptor();
        Properties properties = new Properties();
        properties.setProperty("InjectConditionSQLInterceptor.valueProvider", "com.github.mybatisintercept.InjectConditionSQLInterceptorTest#get");

        interceptor.setProperties(properties);
    }


    public static Object get(String name) {
        return null;
    }
}
