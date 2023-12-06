package com.github.mybatisintercept.util;

import com.github.mybatisintercept.springboot.MybatisInterceptEnvironmentPostProcessor;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.function.Consumer;

public class PlatformDependentUtil {
    public static boolean SPRING_ENVIRONMENT_READY;
    private static Collection<DataSource> SPRING_DATASOURCE_READY;
    public static final boolean EXIST_SPRING_BOOT;
    private static final ArrayList<Runnable> onSpringEnvironmentReadyList = new ArrayList<>();
    private static final ArrayList<Consumer<Collection<DataSource>>> onSpringDatasourceReadyList = new ArrayList<>();

    static {
        boolean existSpringBoot;
        try {
            Class.forName("org.springframework.boot.env.EnvironmentPostProcessor");
            Class.forName("org.springframework.boot.SpringApplication");
            existSpringBoot = true;
        } catch (Throwable e) {
            existSpringBoot = false;
        }
        EXIST_SPRING_BOOT = existSpringBoot;
    }

    public static boolean isMysql(String dbType) {
        return "mysql".equalsIgnoreCase(dbType) || "mariadb".equalsIgnoreCase(dbType);
    }

    public static void onSpringDatasourceReady(Consumer<Collection<DataSource>> consumer) {
        if (SPRING_DATASOURCE_READY != null) {
            consumer.accept(SPRING_DATASOURCE_READY);
        } else {
            onSpringDatasourceReadyList.add(consumer);
        }
    }

    public static void onSpringDatasourceReady(Collection<DataSource> dataSources) {
        SPRING_DATASOURCE_READY = dataSources;
        ArrayList<Consumer<Collection<DataSource>>> consumers = new ArrayList<>(onSpringDatasourceReadyList);
        onSpringDatasourceReadyList.clear();
        onSpringDatasourceReadyList.trimToSize();
        for (Consumer<Collection<DataSource>> runnable : consumers) {
            runnable.accept(dataSources);
        }
    }

    public static void onSpringEnvironmentReady(Runnable runnable) {
        if (SPRING_ENVIRONMENT_READY) {
            runnable.run();
        } else {
            onSpringEnvironmentReadyList.add(runnable);
        }
    }

    public static void onSpringEnvironmentReady() {
        ArrayList<Runnable> list = new ArrayList<>(onSpringEnvironmentReadyList);
        onSpringEnvironmentReadyList.clear();
        onSpringEnvironmentReadyList.trimToSize();
        for (Runnable runnable : list) {
            runnable.run();
        }
    }

    public static Properties resolveSpringPlaceholders(Properties properties, String prefix) {
        if (SPRING_ENVIRONMENT_READY) {
            return MybatisInterceptEnvironmentPostProcessor.resolveSpringPlaceholders(properties, prefix);
        } else {
            return properties;
        }
    }
}
