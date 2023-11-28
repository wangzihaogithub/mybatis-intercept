package com.github.mybatisintercept.springboot;

import com.github.mybatisintercept.util.PlatformDependentUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Collection;

@Component
public class MybatisDatasourceApplicationRunner implements ApplicationRunner {
    @Autowired(required = false)
    private Collection<DataSource> dataSources;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (dataSources != null) {
            PlatformDependentUtil.onSpringDatasourceReady(dataSources);
        }
    }
}
