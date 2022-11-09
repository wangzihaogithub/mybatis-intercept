# mybatis-intercept

#### 介绍
Mybatis拦截器 （可以用于租户隔离）

1. InjectColumnValuesInsertSQLInterceptor.class = 自动给（insert语句, replace语句）加字段

2. InjectColumnValuesUpdateSQLInterceptor.class = 自动给（update语句）加字段属性值， 如果值为空

3. InjectConditionSQLInterceptor.class = 自动给（select语句, update语句, delete语句, insert from语句）加条件

4. InjectMapperParametersInterceptor.class = 给 mapper.xml 加全局内置属性（可以在 mapper.xml 里直接访问这些属性）


#### 软件架构
软件架构说明


#### 安装教程

1.  添加maven依赖, 在pom.xml中加入 [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.wangzihaogithub/mybatis-intercept/badge.svg)](https://search.maven.org/search?q=g:com.github.wangzihaogithub%20AND%20a:mybatis-intercept)


        <!-- https://mvnrepository.com/artifact/com.github.wangzihaogithub/mybatis-intercept -->
        <dependency>
            <groupId>com.github.wangzihaogithub</groupId>
            <artifactId>mybatis-intercept</artifactId>
            <version>1.0.2</version>
        </dependency>
        
2.  配置 mybatis-config.xml

`


        <?xml version="1.0" encoding="UTF-8" ?>
        <!DOCTYPE configuration
                PUBLIC "-//mybatis.org//DTD Config 3.0//EN"
                "http://mybatis.org/dtd/mybatis-3-config.dtd">
        <configuration>
            <plugins>
                <!-- 最后执行 -->
                <plugin interceptor="com.ig.util.IGForceMasterJadeSQLInterceptor"/>
        
                <plugin interceptor="com.github.pagehelper.PageInterceptor">
                    <property name="helperDialect" value="mysql"/>
                    <property name="reasonable" value="false"/>
                    <property name="supportMethodsArguments" value="false"/>
                    <property name="params" value="count=countSql"/>
                </plugin>
        
                <plugin interceptor="com.github.mybatisintercept.InjectColumnValuesInsertSQLInterceptor">
                    <property name="InjectColumnValuesInsertSQLInterceptor.skipTableNames" value="base_holidays,base_subway,base_area,offset,task,hibernate_sequence,help_topic,mapping,sequence_table"/>
                    <property name="InjectColumnValuesInsertSQLInterceptor.valueProvider" value="com.github.securityfilter.util.AccessUserUtil#getAccessUserValue"/>
                    <property name="InjectColumnValuesInsertSQLInterceptor.columnMappings" value="tenant_id=tenantId"/>
                </plugin>
        
                <plugin interceptor="com.github.mybatisintercept.InjectColumnValuesUpdateSQLInterceptor">
                    <property name="InjectColumnValuesUpdateSQLInterceptor.skipTableNames" value="base_holidays,base_subway,base_area,offset,task,hibernate_sequence,help_topic,mapping,sequence_table"/>
                    <property name="InjectColumnValuesUpdateSQLInterceptor.valueProvider" value="com.github.securityfilter.util.AccessUserUtil#getAccessUserValue"/>
                    <property name="InjectColumnValuesUpdateSQLInterceptor.columnMappings" value="tenantId"/>
                </plugin>
        
                <!-- 最先执行 -->
                <plugin interceptor="com.github.mybatisintercept.InjectConditionSQLInterceptor">
                    <property name="InjectConditionSQLInterceptor.skipTableNames" value="base_holidays,base_subway,base_area,offset,task,hibernate_sequence,help_topic,mapping,sequence_table"/>
                    <property name="InjectConditionSQLInterceptor.valueProvider" value="com.github.securityfilter.util.AccessUserUtil#getAccessUserValue"/>
                    <property name="InjectConditionSQLInterceptor.conditionExpression" value="tenant_id = ${tenantId}"/>
                    <property name="InjectConditionSQLInterceptor.existInjectConditionStrategyEnum" value="RULE_TABLE_MATCH_THEN_SKIP_SQL"/>
                </plugin>
            </plugins>
        </configuration>
        
        


        如果是springboot项目，需要在application.yaml里加上
            
        mybatis.config-location: classpath:mybatis-config.xml


`

    

#### 使用说明

1.  xxxx
2.  xxxx
3.  xxxx

#### 参与贡献

1.  Fork 本仓库
2.  新建 Feat_xxx 分支
3.  提交代码
4.  新建 Pull Request


#### 特技

1.  使用 Readme\_XXX.md 来支持不同的语言，例如 Readme\_en.md, Readme\_zh.md
2.  Gitee 官方博客 [blog.gitee.com](https://blog.gitee.com)
3.  你可以 [https://gitee.com/explore](https://gitee.com/explore) 这个地址来了解 Gitee 上的优秀开源项目
4.  [GVP](https://gitee.com/gvp) 全称是 Gitee 最有价值开源项目，是综合评定出的优秀开源项目
5.  Gitee 官方提供的使用手册 [https://gitee.com/help](https://gitee.com/help)
6.  Gitee 封面人物是一档用来展示 Gitee 会员风采的栏目 [https://gitee.com/gitee-stars/](https://gitee.com/gitee-stars/)
