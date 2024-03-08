package com.github.mybatisintercept.springboot;

import com.github.mybatisintercept.InjectConditionSQLInterceptor;
import com.github.mybatisintercept.util.PlatformDependentUtil;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.weaving.LoadTimeWeaverAware;
import org.springframework.core.Ordered;
import org.springframework.instrument.classloading.LoadTimeWeaver;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class MybatisInterceptApplicationContextInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    private final Set<Integer> uniqueBeanSet = Collections.synchronizedSet(new LinkedHashSet<>());

    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
        PreInstantiateSingletonsListener preInstantiateSingletonsListener = new PreInstantiateSingletonsListener(applicationContext);
        if (applicationContext instanceof BeanDefinitionRegistry) {
            ((BeanDefinitionRegistry) applicationContext).registerBeanDefinition(
                    "MybatisInterceptPreInstantiateSingletonsListener",
                    BeanDefinitionBuilder.genericBeanDefinition(LoadTimeWeaverAware.class, () -> preInstantiateSingletonsListener.weaverListener)
                            .getBeanDefinition());
        }
        applicationContext.addApplicationListener(preInstantiateSingletonsListener.appListener);
    }

    private void onBeanFactory(ListableBeanFactory beanFactory) {
        Properties properties = MybatisInterceptEnvironmentPostProcessor.resolveSpringPlaceholders(System.getProperties(), "MybatisInterceptAutoConfiguration.");
        if ("true".equalsIgnoreCase(properties.getProperty("MybatisInterceptAutoConfiguration.block", "true"))) {
            onSpringDatasourceReady(beanFactory);
        } else {
            class MybatisInterceptAutoConfigurationThread extends Thread {
                MybatisInterceptAutoConfigurationThread(Runnable runnable) {
                    super(runnable);
                    setName("MybatisInterceptAutoConfigurationThread");
                    setDaemon(true);
                    setPriority(Thread.MIN_PRIORITY);
                }
            }
            new MybatisInterceptAutoConfigurationThread(() -> onSpringDatasourceReady(beanFactory)).start();
        }
    }

    private void onSpringDatasourceReady(ListableBeanFactory beanFactory) {
        InjectConditionSQLInterceptor.CompileConditionInjectSelector injectSelector = getBean(beanFactory, InjectConditionSQLInterceptor.CompileConditionInjectSelector.class);
        if (injectSelector != null) {
            PlatformDependentUtil.onCompileInjectorReady(injectSelector);
        }

        List<DataSource> dataSourceList;
        try {
            dataSourceList = getDataSourceList(beanFactory);
        } catch (Exception e) {
            if (!PlatformDependentUtil.logError(MybatisInterceptApplicationContextInitializer.class, "getDataSource error = {}", e.toString(), e)) {
                e.printStackTrace();
            }
            return;
        }

        try {
            PlatformDependentUtil.onSpringDatasourceReady(dataSourceList);
        } catch (Exception e) {
            if (!PlatformDependentUtil.logError(MybatisInterceptApplicationContextInitializer.class, "onSpringDatasourceReady error = {}", e.toString(), e)) {
                e.printStackTrace();
            }
        }
        PlatformDependentUtil.onSpringDatasourceReady(dataSourceList);
    }

    private List<DataSource> getDataSourceList(ListableBeanFactory beanFactory) {
        List<DataSource> dataSourceList = new ArrayList<>();
        Map<String, SelectUniqueKeyDatasourceProvider> datasourceProviderMap = getBeansOfType(beanFactory, SelectUniqueKeyDatasourceProvider.class);
        if (datasourceProviderMap.isEmpty()) {
            Map<String, DataSource> dataSourceMap = getBeansOfType(beanFactory, DataSource.class);
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

    private <T> T filter(T bean) {
        if (bean != null && uniqueBeanSet.add(System.identityHashCode(bean))) {
            return bean;
        } else {
            return null;
        }
    }

    private <T> T getBean(ListableBeanFactory beanFactory, Class<T> type) {
        try {
            T bean = beanFactory.getBean(type);
            return filter(bean);
        } catch (NoSuchBeanDefinitionException e) {
            return null;
        }
    }

    private <T> Map<String, T> getBeansOfType(ListableBeanFactory beanFactory, Class<T> type) {
        Map<String, T> map = beanFactory.getBeansOfType(type, false, true);
        Map<String, T> result = new LinkedHashMap<>();
        for (Map.Entry<String, T> entry : map.entrySet()) {
            T filter = filter(entry.getValue());
            if (filter != null) {
                result.put(entry.getKey(), filter);
            }
        }
        return result;
    }

    /**
     * 在单例bean初始化前执行
     */
    private class PreInstantiateSingletonsListener {
        private final ConfigurableApplicationContext applicationContext;
        private final AtomicBoolean once = new AtomicBoolean();
        private final AppListener appListener = new AppListener();
        private final WeaverListener weaverListener = new WeaverListener();

        private PreInstantiateSingletonsListener(ConfigurableApplicationContext applicationContext) {
            this.applicationContext = applicationContext;
        }

        class AppListener implements ApplicationListener<ContextRefreshedEvent>, Ordered {
            @Override
            public void onApplicationEvent(ContextRefreshedEvent event) {
                if (once.compareAndSet(false, true)) {
                    onBeanFactory(event.getApplicationContext());
                }
            }

            @Override
            public int getOrder() {
                return Integer.MIN_VALUE;
            }
        }

        class WeaverListener implements InitializingBean, LoadTimeWeaverAware, Ordered {

            @Override
            public void afterPropertiesSet() throws Exception {
                if (once.compareAndSet(false, true)) {
                    onBeanFactory(applicationContext);
                }
            }

            @Override
            public void setLoadTimeWeaver(LoadTimeWeaver loadTimeWeaver) {

            }

            @Override
            public int getOrder() {
                return Integer.MIN_VALUE;
            }
        }

    }
}
