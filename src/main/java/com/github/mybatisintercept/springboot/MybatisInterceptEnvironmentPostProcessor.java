package com.github.mybatisintercept.springboot;

import com.github.mybatisintercept.util.PlatformDependentUtil;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;

import java.util.Objects;
import java.util.Properties;

@Configuration
public class MybatisInterceptEnvironmentPostProcessor implements EnvironmentPostProcessor {
    private static ConfigurableEnvironment ENV;

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        if (PlatformDependentUtil.SPRING_ENVIRONMENT_READY) {
            return;
        }
        PlatformDependentUtil.SPRING_ENVIRONMENT_READY = true;
        ENV = environment;
        PlatformDependentUtil.onSpringEnvironmentReady();
    }

    public static Properties resolveSpringPlaceholders(Properties properties, String prefix) {
        if (ENV == null) {
            return properties;
        }
        Properties result = null;
        for (Object key : properties.keySet()) {
            if (!(key instanceof String)) {
                continue;
            }
            String value = properties.getProperty(key.toString());
            if (value == null || value.isEmpty()) {
                continue;
            }
            if (!key.toString().startsWith(prefix)) {
                continue;
            }
            try {
                String placeholderValue = ENV.resolvePlaceholders(value);
                if (!Objects.equals(placeholderValue, value)) {
                    if (result == null) {
                        result = new Properties();
                        result.putAll(properties);
                    }
                    result.put(key, placeholderValue);
                }
            } catch (Exception ignored) {

            }
        }
        return result != null ? result : properties;
    }
}
