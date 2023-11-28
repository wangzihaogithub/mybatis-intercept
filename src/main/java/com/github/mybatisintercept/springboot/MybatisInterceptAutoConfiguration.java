package com.github.mybatisintercept.springboot;

import com.github.mybatisintercept.util.PlatformDependentUtil;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.Map;

@AutoConfigureOrder(Integer.MAX_VALUE - 8)
@Configuration
public class MybatisInterceptAutoConfiguration {

    @Bean
    public CommandLineRunner mybatisInterceptCommandLineRunner() {
        return new CommandLineRunner() {
            @Autowired
            private ListableBeanFactory beanFactory;

            @Override
            public void run(String... args) {
                Map<String, DataSource> dataSourceMap = beanFactory.getBeansOfType(DataSource.class, false, true);
                PlatformDependentUtil.onSpringDatasourceReady(dataSourceMap.values());
            }
        };
    }

}
