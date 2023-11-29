package com.github.mybatisintercept.springboot;

import com.github.mybatisintercept.util.PlatformDependentUtil;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@AutoConfigureOrder(Integer.MAX_VALUE - 8)
@Configuration
public class MybatisInterceptAutoConfiguration {

    @Bean
    public CommandLineRunner mybatisInterceptCommandLineRunner() {
        class MybatisInterceptAutoConfigurationThread extends Thread {
            private final ListableBeanFactory beanFactory;

            public MybatisInterceptAutoConfigurationThread(ListableBeanFactory beanFactory) {
                this.beanFactory = beanFactory;
                setName("MybatisInterceptAutoConfigurationThread");
                setDaemon(true);
                setPriority(Thread.MIN_PRIORITY);
            }

            @Override
            public void run() {
                Map<String, SelectUniqueKeyDatasourceProvider> datasourceProviderMap = beanFactory.getBeansOfType(SelectUniqueKeyDatasourceProvider.class, true, true);
                List<DataSource> dataSourceList = new ArrayList<>();
                for (SelectUniqueKeyDatasourceProvider provider : datasourceProviderMap.values()) {
                    DataSource dataSource = provider.getDataSource();
                    if (dataSource != null) {
                        dataSourceList.add(dataSource);
                    }
                }
                if (dataSourceList.isEmpty()) {
                    Map<String, DataSource> dataSourceMap = beanFactory.getBeansOfType(DataSource.class, false, true);
                    dataSourceList.addAll(dataSourceMap.values());
                }
                PlatformDependentUtil.onSpringDatasourceReady(dataSourceList);
            }
        }
        return new CommandLineRunner() {
            @Autowired
            private ListableBeanFactory beanFactory;

            @Override
            public void run(String... args) {
                new MybatisInterceptAutoConfigurationThread(beanFactory).start();
            }
        };
    }

}
