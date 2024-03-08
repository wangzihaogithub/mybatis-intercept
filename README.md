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
            <version>1.0.17</version>
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
                    <property name="params" value="count=countSql"/>
                </plugin>
        
                <plugin interceptor="com.github.mybatisintercept.InjectColumnValuesInsertSQLInterceptor">
                    <property name="InjectColumnValuesInsertSQLInterceptor.skipTableNames" value="biz_remark_content,tenant_position_share,biz_position_open_tenant"/>
                    <property name="InjectColumnValuesInsertSQLInterceptor.valueProvider" value="com.ig.service.framework.AccessUser#getInsertAccessValue"/>
                    <property name="InjectColumnValuesInsertSQLInterceptor.columnMappings" value="tenant_id=tenantId"/>
                </plugin>
        
                <plugin interceptor="com.github.mybatisintercept.InjectColumnValuesUpdateSQLInterceptor">
                    <property name="InjectColumnValuesUpdateSQLInterceptor.skipTableNames" value="${ig.mybatisintercept.skip-table-names}"/>
                    <property name="InjectColumnValuesUpdateSQLInterceptor.valueProvider" value="com.github.securityfilter.util.AccessUserUtil#getAccessUserValue"/>
                    <property name="InjectColumnValuesUpdateSQLInterceptor.columnMappings" value="tenantId"/>
                </plugin>
        
                <!-- 最先执行 -->
                <plugin interceptor="com.github.mybatisintercept.InjectConditionSQLInterceptor">
                    <property name="InjectConditionSQLInterceptor.skipTableNames" value="${ig.mybatisintercept.skip-table-names}"/>
                    <property name="InjectConditionSQLInterceptor.valueProvider" value="com.github.securityfilter.util.AccessUserUtil#getAccessUserValue"/>
                    <property name="InjectConditionSQLInterceptor.conditionExpression" value="
                    tenant_id = ${tenantId};
        
                    tenant_id in (
                        SELECT share_tenant_id from biz_position_open_tenant WHERE position_tenant_id = ${tenantId} and delete_flag = false and biz_position_id = ${unionPositionId}
                        UNION
                        SELECT position_tenant_id from biz_position_open_tenant WHERE share_tenant_id = ${tenantId} and delete_flag = false and biz_position_id = ${unionPositionId}
                        UNION
                        select ${tenantId}
                    );
        
                    tenant_id in (
                        select source_tenant_id from pipeline_talent_corresponding_relation where pipeline_id = ${unionPipelineId} and target_tenant_id = ${tenantId}
                        UNION
                        select target_tenant_id from pipeline_talent_corresponding_relation where pipeline_id = ${unionPipelineId} and source_tenant_id = ${tenantId}
                        UNION
                        select ${tenantId}
                    );
        
                    tenant_id in (
                        select source_tenant_id from pipeline_talent_corresponding_relation where join_id = ${unionJoinId} and target_tenant_id = ${tenantId}
                        UNION
                        select target_tenant_id from pipeline_talent_corresponding_relation where join_id = ${unionJoinId} and source_tenant_id = ${tenantId}
                        UNION
                        select ${tenantId}
                    );
        
                    tenant_id = ${tenantId} or id in (
                        SELECT pm_uid from biz_position_pm WHERE position_id = ${unionPositionId} and delete_flag = false
                    );
        
                    tenant_id = ${tenantId} or id in (
                        SELECT rec_user_id from pipeline WHERE id = ${unionPipelineId}
                        UNION
                        SELECT pm_uid from biz_position_pm WHERE position_id = (SELECT biz_position_id from pipeline WHERE id = ${unionPipelineId} AND delete_flag = false) and delete_flag = false
                        UNION
                        SELECT create_uid from pipeline_operate_log WHERE pipeline_id = ${unionPipelineId}
                        UNION
                        SELECT owner_id from talent WHERE id in (select talent_id from pipeline WHERE id =${unionPipelineId})
                        UNION
                        SELECT bd_uid from biz_corp WHERE id in (select biz_corp_id from pipeline WHERE id =${unionPipelineId})
                    );
        
                    tenant_id = ${tenantId} or id in (
                        SELECT consultant_id from talent_position_join WHERE id = ${unionJoinId}
                        UNION
                        SELECT pm_uid from biz_position_pm WHERE position_id = (SELECT biz_position_id from talent_position_join WHERE id = ${unionJoinId} AND delete_flag = false) and delete_flag = false
                    );
                    "/>
                    <property name="InjectConditionSQLInterceptor.existInjectConditionStrategyEnum" value="RULE_TABLE_MATCH_THEN_SKIP_SQL"/>
                </plugin>

                <plugin interceptor="com.github.mybatisintercept.InjectMapperParametersInterceptor">
                    <property name="InjectMapperParametersInterceptor.attrNames" value="id,tenantId,com.ig.service.framework.AccessUser"/>
                    <property name="InjectMapperParametersInterceptor.valueProvider" value="com.github.securityfilter.util.AccessUserUtil#getAccessUserValue"/>
                    <property name="InjectMapperParametersInterceptor.metaName" value="_meta"/>
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
