package com.github.mybatisintercept.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ASTDruidGetColumnListTest {
    public static void main(String[] args) {
        ASTDruidGetColumnListTest test = new ASTDruidGetColumnListTest();
        test.getColumnList();
    }

    @Test
    public void getColumnList() {
        List<String> columnList = ASTDruidConditionUtil.getColumnList("" +
                "              id in (                 SELECT rec_user_id from pipeline WHERE id = ${unionPipelineId}                 UNION                 SELECT main_pm_id FROM biz_position WHERE id = (SELECT biz_position_id from pipeline WHERE id = ${unionPipelineId} AND delete_flag = false) and delete_flag = false             )"
        );
        Assert.assertEquals(columnList, Collections.singletonList("id"));

        List<String> columnList1 = ASTDruidConditionUtil.getColumnList("" +
                "       tenant_id = ${tenantId} or tenant_id in (SELECT position_tenant_id from biz_position_open_tenant WHERE share_tenant_id = ${tenantId} and delete_flag = false and biz_position_id = ${unionPositionId})"
        );
        Assert.assertEquals(columnList1, Arrays.asList("tenant_id","tenant_id"));

    }
}
