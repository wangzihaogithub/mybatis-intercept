package com.github.mybatisintercept.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SQLCompileTest {
    public static void main(String[] args) {
        SQLCompileTest test = new SQLCompileTest();
        test.compile();
    }

    @Test
    public void compile() {
        SQL empty = SQL.compile("", Collections.singletonMap("tenantId", 1));
        Assert.assertEquals("", empty.getExprSql());

        SQL compile1 = SQL.compile("tenant_id = '${tenantId}'", Collections.singletonMap("tenantId", "1"));
        Assert.assertEquals("tenant_id = '1'", compile1.getExprSql());

        List<String> columnList = ASTDruidConditionUtil.getColumnList(compile1.getExprSql());
        Assert.assertEquals(Collections.singletonList("tenant_id"), columnList);

        Assert.assertEquals("compile1.getArgNameAndDefaultValues().size() == 1'", 1, compile1.getArgNameAndDefaultValues().size());

        SQL compile2 = SQL.compile("tenant_id = '${tenantId}' and id > ${id|2}", Collections.singletonMap("tenantId", "1"));
        Assert.assertEquals("tenant_id = '1' and id > 2", compile2.getExprSql());

        SQL compile3 = SQL.compile("tenant_id = '${tenantId}' and xx_name like '%${xxName}%'", Collections.singletonMap("tenantId", "1"));
        Assert.assertEquals("tenant_id = '1' and xx_name like '%%'", compile3.getExprSql());

        List<String> columnList1 = ASTDruidConditionUtil.getColumnList(compile3.getExprSql());
        Assert.assertEquals(Arrays.asList("tenant_id", "xx_name"), columnList1);

        SQL compile33 = SQL.compile("(tenant_id = 1 or tenant_id = '${tenantId}') and dept_id in (select id from dept where type = 1 and tenant_id in (${tenantId}))and xx_name like '%${xxName}%'", Collections.singletonMap("tenantId", "1"));
        Assert.assertEquals("(tenant_id = 1 or tenant_id = '1') and dept_id in (select id from dept where type = 1 and tenant_id in (1))and xx_name like '%%'", compile33.getExprSql());

        List<String> columnList2 = ASTDruidConditionUtil.getColumnList(compile33.getExprSql());
        Assert.assertEquals(Arrays.asList("tenant_id", "tenant_id", "dept_id", "xx_name"), columnList2);

        SQL compile4 = SQL.compile("tenant_id = '${tenantId}' and xx_name like '%${xxName|2}%'", Collections.singletonMap("tenantId", "1"));
        Assert.assertEquals("tenant_id = '1' and xx_name like '%2%'", compile4.getExprSql());
    }

}
