package com.github.mybatisintercept.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class SQLTest {
    public static void main(String[] args) {
        SQLTest test = new SQLTest();
        test.compile();
    }

    @Test
    public void compile() {
        SQL compile1 = SQL.compile("tenant_id = '${tenantId}'", Collections.singletonMap("tenantId", "1"));
        Assert.assertEquals("tenant_id = '1'", compile1.getExprSql());

        SQL compile2 = SQL.compile("tenant_id = '${tenantId}' and id > ${id|2}", Collections.singletonMap("tenantId", "1"));
        Assert.assertEquals("tenant_id = '1' and id > 2", compile2.getExprSql());

        SQL compile3 = SQL.compile("tenant_id = '${tenantId}' and xx_name like '%${xxName}%'", Collections.singletonMap("tenantId", "1"));
        Assert.assertEquals("tenant_id = '1' and xx_name like '%%'", compile3.getExprSql());

        SQL compile4 = SQL.compile("tenant_id = '${tenantId}' and xx_name like '%${xxName|2}%'", Collections.singletonMap("tenantId", "1"));
        Assert.assertEquals("tenant_id = '1' and xx_name like '%2%'", compile4.getExprSql());
    }

}
