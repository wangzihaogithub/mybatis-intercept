package com.github.mybatisintercept.springboot;

import javax.sql.DataSource;

public interface SelectUniqueKeyDatasourceProvider {
    DataSource getDataSource();
}
