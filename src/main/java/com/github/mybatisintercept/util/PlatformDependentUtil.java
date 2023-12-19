package com.github.mybatisintercept.util;

import com.github.mybatisintercept.InjectConditionSQLInterceptor;
import com.github.mybatisintercept.springboot.MybatisInterceptEnvironmentPostProcessor;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.function.Consumer;

public class PlatformDependentUtil {
    public static boolean SPRING_ENVIRONMENT_READY;
    public static final boolean EXIST_SPRING_BOOT;
    private static Collection<DataSource> SPRING_DATASOURCE_READY;
    private static InjectConditionSQLInterceptor.CompileConditionInjectSelector COMPILE_INJECTOR_SELECTOR;
    private static final ArrayList<Consumer<InjectConditionSQLInterceptor.CompileConditionInjectSelector>> onCompileInjectorSelectorReadyList = new ArrayList<>();
    private static final ArrayList<Runnable> onSpringEnvironmentReadyList = new ArrayList<>();
    private static final ArrayList<Consumer<Collection<DataSource>>> onSpringDatasourceReadyList = new ArrayList<>();
    private static final Method METHOD_GET_LOGGER;
    private static final Method METHOD_LOGGER_ERROR;

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

        Method loggerFactoryGetLogger;
        Method loggerError;
        try {
            loggerFactoryGetLogger = Class.forName("org.slf4j.LoggerFactory").getDeclaredMethod("getLogger", Class.class);
            loggerError = Class.forName("org.slf4j.Logger").getDeclaredMethod("error", String.class, Object[].class);
        } catch (Throwable e) {
            loggerFactoryGetLogger = null;
            loggerError = null;
        }
        METHOD_GET_LOGGER = loggerFactoryGetLogger;
        METHOD_LOGGER_ERROR = loggerError;
    }

    public static boolean logError(Class<?> clazz, String format, Object... args) {
        if (METHOD_LOGGER_ERROR != null) {
            try {
                Object logger = METHOD_GET_LOGGER.invoke(null, clazz);
                METHOD_LOGGER_ERROR.invoke(logger, format, args);
                return true;
            } catch (Exception ignored) {
            }
        }
        return false;
    }

    public static <E extends Throwable> void sneakyThrows(Throwable t) throws E {
        throw (E) t;
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

    public static void onCompileInjectorReady(InjectConditionSQLInterceptor.CompileConditionInjectSelector injectSelector) {
        ArrayList<Consumer<InjectConditionSQLInterceptor.CompileConditionInjectSelector>> consumers = new ArrayList<>(onCompileInjectorSelectorReadyList);
        onCompileInjectorSelectorReadyList.clear();
        onCompileInjectorSelectorReadyList.trimToSize();
        for (Consumer<InjectConditionSQLInterceptor.CompileConditionInjectSelector> runnable : consumers) {
            runnable.accept(injectSelector);
        }
        COMPILE_INJECTOR_SELECTOR = injectSelector;
    }

    public static void onCompileInjectorReady(Consumer<InjectConditionSQLInterceptor.CompileConditionInjectSelector> consumer) {
        if (COMPILE_INJECTOR_SELECTOR != null) {
            consumer.accept(COMPILE_INJECTOR_SELECTOR);
        } else {
            onCompileInjectorSelectorReadyList.add(consumer);
        }
    }

    public static void onSpringDatasourceReady(Collection<DataSource> dataSources) {
        ArrayList<Consumer<Collection<DataSource>>> consumers = new ArrayList<>(onSpringDatasourceReadyList);
        onSpringDatasourceReadyList.clear();
        onSpringDatasourceReadyList.trimToSize();
        for (Consumer<Collection<DataSource>> runnable : consumers) {
            runnable.accept(dataSources);
        }
        SPRING_DATASOURCE_READY = dataSources;
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
