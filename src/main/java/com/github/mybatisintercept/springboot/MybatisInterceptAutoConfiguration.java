package com.github.mybatisintercept.springboot;

import com.github.mybatisintercept.InjectConditionSQLInterceptor;
import com.github.mybatisintercept.util.PlatformDependentUtil;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@AutoConfigureOrder(Integer.MAX_VALUE - 8)
@Configuration
public class MybatisInterceptAutoConfiguration {

    @Bean
    public CommandLineRunner mybatisInterceptCommandLineRunner() {
        class MybatisInterceptAutoConfigurationRunnable implements Runnable {
            private final ListableBeanFactory beanFactory;

            public MybatisInterceptAutoConfigurationRunnable(ListableBeanFactory beanFactory) {
                this.beanFactory = beanFactory;
            }

            private <T> T getBean(Class<T> type) {
                try {
                    return beanFactory.getBean(type);
                } catch (NoSuchBeanDefinitionException e) {
                    return null;
                }
            }

            private List<DataSource> getDataSourceList() {
                List<DataSource> dataSourceList = new ArrayList<>();
                Map<String, SelectUniqueKeyDatasourceProvider> datasourceProviderMap = beanFactory.getBeansOfType(SelectUniqueKeyDatasourceProvider.class, true, true);
                if (datasourceProviderMap.isEmpty()) {
                    Map<String, DataSource> dataSourceMap = beanFactory.getBeansOfType(DataSource.class, false, true);
                    dataSourceList.addAll(dataSourceMap.values());
                } else {
                    for (SelectUniqueKeyDatasourceProvider provider : datasourceProviderMap.values()) {
                        DataSource dataSource = provider.getDataSource();
                        if (dataSource != null) {
                            dataSourceList.add(dataSource);
                        }
                    }
                }
                return dataSourceList;
            }

            @Override
            public void run() {
                InjectConditionSQLInterceptor.CompileConditionInjectSelector injectSelector = getBean(InjectConditionSQLInterceptor.CompileConditionInjectSelector.class);
                if (injectSelector != null) {
                    PlatformDependentUtil.onCompileInjectorReady(injectSelector);
                }

                List<DataSource> dataSourceList;
                try {
                    dataSourceList = getDataSourceList();
                } catch (Exception e) {
                    if (!PlatformDependentUtil.logError(MybatisInterceptAutoConfigurationRunnable.class, "getDataSource error = {}", e.toString(), e)) {
                        e.printStackTrace();
                    }
                    return;
                }

                try {
                    PlatformDependentUtil.onSpringDatasourceReady(dataSourceList);
                } catch (Exception e) {
                    if (!PlatformDependentUtil.logError(MybatisInterceptAutoConfigurationRunnable.class, "onSpringDatasourceReady error = {}", e.toString(), e)) {
                        e.printStackTrace();
                    }
                }
                PlatformDependentUtil.onSpringDatasourceReady(dataSourceList);
            }
        }
        return new CommandLineRunner() {
            @Autowired
            private ListableBeanFactory beanFactory;

            @Override
            public void run(String... args) {
                Properties properties = MybatisInterceptEnvironmentPostProcessor.resolveSpringPlaceholders(System.getProperties(), "MybatisInterceptAutoConfiguration.");
                boolean block = "true".equalsIgnoreCase(properties.getProperty("MybatisInterceptAutoConfiguration.block", "true"));

                Runnable runnable = new MybatisInterceptAutoConfigurationRunnable(beanFactory);
                if (block) {
                    runnable.run();
                } else {
                    class MybatisInterceptAutoConfigurationThread extends Thread {
                        MybatisInterceptAutoConfigurationThread(Runnable runnable) {
                            super(runnable);
                            setName("MybatisInterceptAutoConfigurationThread");
                            setDaemon(true);
                            setPriority(Thread.MIN_PRIORITY);
                        }
                    }
                    new MybatisInterceptAutoConfigurationThread(runnable).start();
                }
            }
        };
    }

}
