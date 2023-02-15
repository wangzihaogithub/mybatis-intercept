package com.github.mybatisintercept.util;

import com.github.mybatisintercept.springboot.MybatisInterceptEnvironmentPostProcessor;

import java.util.ArrayList;
import java.util.Properties;

public class PlatformDependentUtil {
    public static boolean SPRING_ENVIRONMENT_READY;
    public static final boolean EXIST_SPRING_BOOT;
    private static final ArrayList<Runnable> onSpringEnvironmentReadyList = new ArrayList<>();

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

    public static void onSpringEnvironmentReady(Runnable runnable) {
        if (SPRING_ENVIRONMENT_READY) {
            runnable.run();
        } else {
            onSpringEnvironmentReadyList.add(runnable);
        }
    }

    public static void onSpringEnvironmentReady() {
        for (Runnable runnable : onSpringEnvironmentReadyList) {
            runnable.run();
        }
        onSpringEnvironmentReadyList.clear();
        onSpringEnvironmentReadyList.trimToSize();
    }

    public static Properties resolveSpringPlaceholders(Properties properties, String prefix) {
        if (SPRING_ENVIRONMENT_READY) {
            return MybatisInterceptEnvironmentPostProcessor.resolveSpringPlaceholders(properties, prefix);
        } else {
            return properties;
        }
    }
}
