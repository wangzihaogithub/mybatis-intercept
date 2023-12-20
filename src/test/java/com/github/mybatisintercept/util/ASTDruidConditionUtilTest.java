package com.github.mybatisintercept.util;

import org.junit.Assert;
import org.junit.Test;

public class ASTDruidConditionUtilTest {

    public static void main(String[] args) {
        ASTDruidConditionUtilTest test = new ASTDruidConditionUtilTest();

        test.select();
        test.mysql8Cte();
        test.update();
        test.delete();
        test.insert();
        System.out.println();
    }

    @Test
    public void mysql8Cte() {
        String cte1 = ASTDruidTestUtil.addAndCondition("with user_cte(dept_id,name) as (select dept_id,name from user ) SELECT t1.id, t2.* " +
                "from dept t1 left join user_cte t2 on t1.id = t2.dept_id", "tenant_id = 2");
        Assert.assertEquals("WITH user_cte (dept_id, name) AS (\n" +
                "\t\tSELECT dept_id, name\n" +
                "\t\tFROM user\n" +
                "\t\tWHERE user.tenant_id = 2\n" +
                "\t)\n" +
                "SELECT t1.id, t2.*\n" +
                "FROM dept t1\n" +
                "\tLEFT JOIN user_cte t2\n" +
                "\tON t1.id = t2.dept_id\n" +
                "\t\tAND t2.tenant_id = 2\n" +
                "WHERE t1.tenant_id = 2", cte1);

        String cte2 = ASTDruidTestUtil.addAndCondition(
                "WITH user_cte (dept_id, name) AS (SELECT dept_id, name FROM user)," +
                        " user_cte2 (dept_id, name) AS (SELECT dept_id, name FROM user) " +
                        "SELECT t1.id, t2.* " +
                        "FROM dept t1 " +
                        "LEFT JOIN user_cte t2 ON t1.id = t2.dept_id " +
                        "LEFT JOIN user_cte2 t3 ON t1.id = t3.dept_id", "tenant_id = 2");
        Assert.assertEquals("WITH user_cte (dept_id, name) AS (\n" +
                "\t\tSELECT dept_id, name\n" +
                "\t\tFROM user\n" +
                "\t\tWHERE user.tenant_id = 2\n" +
                "\t), \n" +
                "\tuser_cte2 (dept_id, name) AS (\n" +
                "\t\tSELECT dept_id, name\n" +
                "\t\tFROM user\n" +
                "\t\tWHERE user.tenant_id = 2\n" +
                "\t)\n" +
                "SELECT t1.id, t2.*\n" +
                "FROM dept t1\n" +
                "\tLEFT JOIN user_cte t2\n" +
                "\tON t1.id = t2.dept_id\n" +
                "\t\tAND t2.tenant_id = 2\n" +
                "\tLEFT JOIN user_cte2 t3\n" +
                "\tON t1.id = t3.dept_id\n" +
                "\t\tAND t3.tenant_id = 2\n" +
                "WHERE t1.tenant_id = 2", cte2);
        System.out.println("mysql8Cte");
    }

    @Test
    public void select() {
        String injectConditionNfq2 = ASTDruidTestUtil.addAndCondition("SELECT *  FROM   p_user pu  LEFT JOIN p_dept pd ON pu.dept_id = pd.id where pu.id = ?",
                "tenant_id =1 or tenant_id in (select tenant_id from tenant_scope where type = 1 and xx=${x} ) ");
        Assert.assertEquals("SELECT *\n" +
                "FROM p_user pu\n" +
                "\tLEFT JOIN p_dept pd\n" +
                "\tON pu.dept_id = pd.id\n" +
                "\t\tAND (pd.tenant_id = 1\n" +
                "\t\t\tOR pd.tenant_id IN (\n" +
                "\t\t\t\tSELECT tenant_id\n" +
                "\t\t\t\tFROM tenant_scope\n" +
                "\t\t\t\tWHERE type = 1\n" +
                "\t\t\t\t\tAND xx = ${x}\n" +
                "\t\t\t))\n" +
                "WHERE pu.id = ?\n" +
                "\tAND (pu.tenant_id = 1\n" +
                "\t\tOR pu.tenant_id IN (\n" +
                "\t\t\tSELECT tenant_id\n" +
                "\t\t\tFROM tenant_scope\n" +
                "\t\t\tWHERE type = 1\n" +
                "\t\t\t\tAND xx = ${x}\n" +
                "\t\t))", injectConditionNfq2);

        String injectConditionNq = ASTDruidTestUtil.addAndCondition("select t1.a, t2.b from user t1 left join dept t2 on t1.dept_id = t2.id where t1.id = ?",
                "tenant_id =1 or tenant_id in (select tenant_id from tenant_scope where type = 1 and xx=${x} ) ");
        Assert.assertEquals("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tLEFT JOIN dept t2\n" +
                "\tON t1.dept_id = t2.id\n" +
                "\t\tAND (t2.tenant_id = 1\n" +
                "\t\t\tOR t2.tenant_id IN (\n" +
                "\t\t\t\tSELECT tenant_id\n" +
                "\t\t\t\tFROM tenant_scope\n" +
                "\t\t\t\tWHERE type = 1\n" +
                "\t\t\t\t\tAND xx = ${x}\n" +
                "\t\t\t))\n" +
                "WHERE t1.id = ?\n" +
                "\tAND (t1.tenant_id = 1\n" +
                "\t\tOR t1.tenant_id IN (\n" +
                "\t\t\tSELECT tenant_id\n" +
                "\t\t\tFROM tenant_scope\n" +
                "\t\t\tWHERE type = 1\n" +
                "\t\t\t\tAND xx = ${x}\n" +
                "\t\t))", injectConditionNq);

        String injectConditionNq2 = ASTDruidTestUtil.addAndCondition("select t1.a, t2.b from user t1 left join dept t2 on t1.dept_id = t2.id where t1.id = ?",
                "tenant_id =1 or tenant_id in (1) ");
        Assert.assertEquals("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tLEFT JOIN dept t2\n" +
                "\tON t1.dept_id = t2.id\n" +
                "\t\tAND (t2.tenant_id = 1\n" +
                "\t\t\tOR tenant_id IN (1))\n" +
                "WHERE t1.id = ?\n" +
                "\tAND (t1.tenant_id = 1\n" +
                "\t\tOR tenant_id IN (1))", injectConditionNq2);

        String injectConditionNq23 = ASTDruidTestUtil.addAndCondition("select t1.a, t2.b from user t1 left join dept t2 on t1.dept_id = t2.id where t1.id = ?",
                "tenant_id in (1,2) ");
        Assert.assertEquals("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tLEFT JOIN dept t2\n" +
                "\tON t2.dept_id = t2.id\n" +
                "\t\tAND tenant_id IN (1, 2)\n" +
                "WHERE t1.id = ?\n" +
                "\tAND tenant_id IN (1, 2)", injectConditionNq23);

        String injectConditionNq1 = ASTDruidTestUtil.addAndCondition("select t1.a, t2.b from user t1 left join dept t2 on t1.dept_id = t2.id where t1.id = ?",
                "tenant_id in (select tenant_id from tenant_scope where type = 1 and xx=${x} ) ");
        Assert.assertEquals("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tLEFT JOIN dept t2\n" +
                "\tON t2.dept_id = t2.id\n" +
                "\t\tAND t2.tenant_id IN (\n" +
                "\t\t\tSELECT tenant_id\n" +
                "\t\t\tFROM tenant_scope\n" +
                "\t\t\tWHERE type = 1\n" +
                "\t\t\t\tAND xx = ${x}\n" +
                "\t\t)\n" +
                "WHERE t1.id = ?\n" +
                "\tAND t1.tenant_id IN (\n" +
                "\t\tSELECT tenant_id\n" +
                "\t\tFROM tenant_scope\n" +
                "\t\tWHERE type = 1\n" +
                "\t\t\tAND xx = ${x}\n" +
                "\t)", injectConditionNq1);

        String bug1 = ASTDruidTestUtil.addAndCondition("SELECT\n" +
                        "\tbp.id AS positionId,\n" +
                        "\tbp.NAME AS positionName,\n" +
                        "\t( SELECT GROUP_CONCAT( dar.role ) FROM data_achieve_user_role dar WHERE dar.data_achieve_id = a.id AND dar.delete_flag = 0 AND dar.tenant_id = 1 ) AS roleCode,\n" +
                        "IF\n" +
                        "\t( pi.source = 'gulu', bc.brand, n.NAME ) AS baseNodeName,\n" +
                        "\tpi.id AS id,\n" +
                        "\tDATE_FORMAT( pipeext.upload_time, '%Y-%m-%d' ) AS uploadTime,\n" +
                        "\tDATE_FORMAT( pi.create_time, '%Y-%m-%d' ) AS createTime,\n" +
                        "\tDATE_FORMAT( pi.invoice_time, '%Y-%m-%d' ) AS invoiceTime,\n" +
                        "\tDATE_FORMAT( pi.send_time, '%Y-%m-%d' ) AS sendTime,\n" +
                        "\tDATE_FORMAT( pi.pay_time, '%Y-%m-%d' ) AS payTime,\n" +
                        "\tDATE_FORMAT( pi.sign_time, '%Y-%m-%d' ) AS signTime,\n" +
                        "\tDATE_FORMAT( pi.entry_time, '%Y-%m-%d' ) AS entryTime,\n" +
                        "\tpi.STATUS AS STATUS,\n" +
                        "\tbc.NAME AS corpName,\n" +
                        "\tbc.brand AS brand,\n" +
                        "\tt.talent_name AS talentName,\n" +
                        "\tt.id AS talentId,\n" +
                        "\ta.money AS money,\n" +
                        "\ta.after_tax_money AS afterTaxMoney,\n" +
                        "\tp1.name_en AS userName,\n" +
                        "\ta.p_user_id AS userId,\n" +
                        "\tbc.id AS corpId,\n" +
                        "\tpi.source AS source \n" +
                        "FROM\n" +
                        "\tdata_achieve a\n" +
                        "\tLEFT JOIN pipeline_invoice pi ON a.invoice_id = pi.id\n" +
                        "\tLEFT JOIN p_user p1 ON a.p_user_id = p1.id\n" +
                        "\tLEFT JOIN biz_corp bc ON pi.biz_corp_id = bc.id\n" +
                        "\tLEFT JOIN biz_position bp ON pi.biz_position_id = bp.id\n" +
                        "\tLEFT JOIN base_node n ON bp.base_node_id = n.id\n" +
                        "\tLEFT JOIN talent t ON pi.talent_id = t.id\n" +
                        "\tLEFT JOIN pipeline_ext pipeext ON pipeext.pipeline_id = pi.pipeline_id \n" +
                        "WHERE\n" +
                        "\t(\n" +
                        "\t\ta.delete_flag = 0 \n" +
                        "\t\tAND pi.delete_flag = 0 \n" +
                        "\t\tAND a.money != 0 \n" +
                        "\t\tAND a.dept_id IN (123) \n" +
                        "\t\tAND pi.STATUS != 270 \n" +
                        "\t\tAND (\n" +
                        "\t\t\tEXISTS (\n" +
                        "\t\t\tSELECT\n" +
                        "\t\t\t\t* \n" +
                        "\t\t\tFROM\n" +
                        "\t\t\t\tpipeline_ext pe \n" +
                        "\t\t\tWHERE\n" +
                        "\t\t\t\tpe.pipeline_id = pi.pipeline_id \n" +
                        "\t\t\t\tAND pe.first_interview_time <= ? AND pe.first_interview_time >= ? \n" +
                        "\t\t\t\tAND pe.tenant_id = 1 \n" +
                        "\t\t\t) \n" +
                        "\t\t\tAND pi.payment_type = 2 \n" +
                        "\t\t) \n" +
                        "\tOR ( pi.sign_time <= ? AND pi.sign_time >= ? AND pi.payment_type = 1 )\n" +
                        "\t) \n" +
                        "\tAND a.tenant_id = 1 \n" +
                        "ORDER BY\n" +
                        "\tpi.create_time DESC \n" +
                        "\tLIMIT ?", "tenant_id = 2",
                ExistInjectConditionStrategyEnum.RULE_TABLE_MATCH_THEN_SKIP_ITEM);

        Assert.assertEquals("SELECT bp.id AS positionId, bp.NAME AS positionName\n" +
                "\t, (\n" +
                "\t\tSELECT GROUP_CONCAT(dar.role)\n" +
                "\t\tFROM data_achieve_user_role dar\n" +
                "\t\tWHERE dar.data_achieve_id = a.id\n" +
                "\t\t\tAND dar.delete_flag = 0\n" +
                "\t\t\tAND dar.tenant_id = 1\n" +
                "\t) AS roleCode\n" +
                "\t, IF(pi.source = 'gulu', bc.brand, n.NAME) AS baseNodeName\n" +
                "\t, pi.id AS id, DATE_FORMAT(pipeext.upload_time, '%Y-%m-%d') AS uploadTime\n" +
                "\t, DATE_FORMAT(pi.create_time, '%Y-%m-%d') AS createTime\n" +
                "\t, DATE_FORMAT(pi.invoice_time, '%Y-%m-%d') AS invoiceTime\n" +
                "\t, DATE_FORMAT(pi.send_time, '%Y-%m-%d') AS sendTime\n" +
                "\t, DATE_FORMAT(pi.pay_time, '%Y-%m-%d') AS payTime\n" +
                "\t, DATE_FORMAT(pi.sign_time, '%Y-%m-%d') AS signTime\n" +
                "\t, DATE_FORMAT(pi.entry_time, '%Y-%m-%d') AS entryTime, pi.STATUS AS STATUS\n" +
                "\t, bc.NAME AS corpName, bc.brand AS brand, t.talent_name AS talentName, t.id AS talentId, a.money AS money\n" +
                "\t, a.after_tax_money AS afterTaxMoney, p1.name_en AS userName, a.p_user_id AS userId, bc.id AS corpId, pi.source AS source\n" +
                "FROM data_achieve a\n" +
                "\tLEFT JOIN pipeline_invoice pi\n" +
                "\tON a.invoice_id = pi.id\n" +
                "\t\tAND pi.tenant_id = 2\n" +
                "\tLEFT JOIN p_user p1\n" +
                "\tON a.p_user_id = p1.id\n" +
                "\t\tAND p1.tenant_id = 2\n" +
                "\tLEFT JOIN biz_corp bc\n" +
                "\tON pi.biz_corp_id = bc.id\n" +
                "\t\tAND bc.tenant_id = 2\n" +
                "\tLEFT JOIN biz_position bp\n" +
                "\tON pi.biz_position_id = bp.id\n" +
                "\t\tAND bp.tenant_id = 2\n" +
                "\tLEFT JOIN base_node n\n" +
                "\tON bp.base_node_id = n.id\n" +
                "\t\tAND n.tenant_id = 2\n" +
                "\tLEFT JOIN talent t\n" +
                "\tON pi.talent_id = t.id\n" +
                "\t\tAND t.tenant_id = 2\n" +
                "\tLEFT JOIN pipeline_ext pipeext\n" +
                "\tON pipeext.pipeline_id = pi.pipeline_id\n" +
                "\t\tAND pipeext.tenant_id = 2\n" +
                "WHERE (a.delete_flag = 0\n" +
                "\t\tAND pi.delete_flag = 0\n" +
                "\t\tAND a.money != 0\n" +
                "\t\tAND a.dept_id IN (123)\n" +
                "\t\tAND pi.STATUS != 270\n" +
                "\t\tAND (EXISTS (\n" +
                "\t\t\t\tSELECT *\n" +
                "\t\t\t\tFROM pipeline_ext pe\n" +
                "\t\t\t\tWHERE pe.pipeline_id = pi.pipeline_id\n" +
                "\t\t\t\t\tAND pe.first_interview_time <= ?\n" +
                "\t\t\t\t\tAND pe.first_interview_time >= ?\n" +
                "\t\t\t\t\tAND pe.tenant_id = 1\n" +
                "\t\t\t)\n" +
                "\t\t\tAND pi.payment_type = 2)\n" +
                "\t\tOR (pi.sign_time <= ?\n" +
                "\t\t\tAND pi.sign_time >= ?\n" +
                "\t\t\tAND pi.payment_type = 1))\n" +
                "\tAND a.tenant_id = 1\n" +
                "ORDER BY pi.create_time DESC\n" +
                "LIMIT ?", bug1);

        String self = ASTDruidTestUtil.addAndCondition(" SELECT\n" +
                        "            a.id,\n" +
                        "            a.NAME,\n" +
                        "            a.short_name as shortName,\n" +
                        "            a.pinyin pinyin,\n" +
                        "            a.area_parent AS parentId,\n" +
                        "            a.first_pinyin as firstPinyin,\n" +
                        "            b.`name` as parentName,\n" +
                        "            b.short_name as parentShortName\n" +
                        "        FROM\n" +
                        "            base_area a left join base_area b on a.area_parent = b.id\n" +
                        "        WHERE\n" +
                        "            a.type = 3\n" +
                        "          and a.is_delete = 0\n" +
                        "          and a.hot_flag = 1\n" +
                        "        ORDER BY a.pinyin", "tenant_id = 2",
                ExistInjectConditionStrategyEnum.RULE_TABLE_MATCH_THEN_SKIP_SQL,
                "mysql",
                (s, t) -> {
                    return t.equals("base_area");
                });

        Assert.assertEquals(" SELECT\n" +
                "            a.id,\n" +
                "            a.NAME,\n" +
                "            a.short_name as shortName,\n" +
                "            a.pinyin pinyin,\n" +
                "            a.area_parent AS parentId,\n" +
                "            a.first_pinyin as firstPinyin,\n" +
                "            b.`name` as parentName,\n" +
                "            b.short_name as parentShortName\n" +
                "        FROM\n" +
                "            base_area a left join base_area b on a.area_parent = b.id\n" +
                "        WHERE\n" +
                "            a.type = 3\n" +
                "          and a.is_delete = 0\n" +
                "          and a.hot_flag = 1\n" +
                "        ORDER BY a.pinyin", self);

        String from = ASTDruidTestUtil.addAndCondition("SELECT\n" +
                "\ta.id,\n" +
                "\ta.oid,\n" +
                "\ta.rid,\n" +
                "\ta.type,\n" +
                "\tr.NAME,\n" +
                "\tr.description,\n" +
                "\tr.roles \n" +
                "FROM\n" +
                "\tp_role_acl a,\n" +
                "\tp_role r " +
                "WHERE\n" +
                "\ta.oid = ? \n" +
                "\tAND a.type = ? \n" +
                "\tAND a.rid = r.id \n" +
                "\tAND a.is_delete = 0 \n" +
                "\tAND r.is_delete = 0", "tenant_id = 2");
        Assert.assertEquals("SELECT a.id, a.oid, a.rid, a.type, r.NAME\n" +
                "\t, r.description, r.roles\n" +
                "FROM p_role_acl a, p_role r\n" +
                "WHERE a.oid = ?\n" +
                "\tAND a.type = ?\n" +
                "\tAND a.rid = r.id\n" +
                "\tAND a.is_delete = 0\n" +
                "\tAND r.is_delete = 0\n" +
                "\tAND r.tenant_id = 2\n" +
                "\tAND a.tenant_id = 2", from);

        String injectConditionN = ASTDruidTestUtil.addAndCondition("select t1.a, t2.b from user t1 left join dept t2 on t1.dept_id = t2.id where t1.id = ?",
                "tenant_id = 2 and name like 'a' or b > 10");
        Assert.assertEquals("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tLEFT JOIN dept t2\n" +
                "\tON t1.dept_id = t2.id\n" +
                "\t\tAND (t2.tenant_id = 2\n" +
                "\t\t\tAND t2.name LIKE 'a'\n" +
                "\t\t\tOR t2.b > 10)\n" +
                "WHERE t1.id = ?\n" +
                "\tAND (t1.tenant_id = 2\n" +
                "\t\tAND t1.name LIKE 'a'\n" +
                "\t\tOR t1.b > 10)", injectConditionN);

        String wherePart = ASTDruidTestUtil.addAndCondition("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tLEFT JOIN dept t2\n" +
                "\tON t1.dept_id = t2.id\n" +
                "\t\tAND t2.tenant_id = 2 and t2.type = 1\n" +
                "WHERE t1.id = ?  and t1.type = 1", "tenant_id = 1");
        Assert.assertEquals("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tLEFT JOIN dept t2\n" +
                "\tON t1.dept_id = t2.id\n" +
                "\t\tAND t2.tenant_id = 2 and t2.type = 1\n" +
                "WHERE t1.id = ?  and t1.type = 1", wherePart);

        String where = ASTDruidTestUtil.addAndCondition("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tLEFT JOIN dept t2\n" +
                "\tON t1.dept_id = t2.id\n" +
                "\t\tAND t2.tenant_id = 2 and t2.type = 1\n" +
                "WHERE t1.id = ?  and t1.type = 1\n" +
                "\tAND t1.tenant_id = 2", "tenant_id = 1");
        Assert.assertEquals("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tLEFT JOIN dept t2\n" +
                "\tON t1.dept_id = t2.id\n" +
                "\t\tAND t2.tenant_id = 2 and t2.type = 1\n" +
                "WHERE t1.id = ?  and t1.type = 1\n" +
                "\tAND t1.tenant_id = 2", where);

        String insert3 = ASTDruidTestUtil.addAndCondition("SELECT * INTO p_user_1 FROM p_user  ", "tenant_id = 2");
        Assert.assertEquals("SELECT *\n" +
                "INTO p_user_1\n" +
                "FROM p_user\n" +
                "WHERE p_user.tenant_id = 2", insert3);

        String set = ASTDruidTestUtil.addAndCondition("set global query_cache_type = OFF ", "tenant_id = 2");
        Assert.assertEquals("set global query_cache_type = OFF ", set);

        String at = ASTDruidTestUtil.addAndCondition("SELECT @@auto_generate_certs ", "tenant_id = 2");
        Assert.assertEquals("SELECT @@auto_generate_certs ", at);

        String show = ASTDruidTestUtil.addAndCondition("SHOW VARIABLES ", "tenant_id = 2");
        Assert.assertEquals("SHOW VARIABLES ", show);

        String dual = ASTDruidTestUtil.addAndCondition("SELECT 1 FROM DUAL ", "tenant_id = 2");
        Assert.assertEquals("SELECT 1 FROM DUAL ", dual);

        // join 1个表
        String join1 = ASTDruidTestUtil.addAndCondition("select t1.a, t2.b from user t1 left join dept t2 on t1.dept_id = t2.id where t1.id = ?", "tenant_id = 2");
        Assert.assertEquals("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tLEFT JOIN dept t2\n" +
                "\tON t1.dept_id = t2.id\n" +
                "\t\tAND t2.tenant_id = 2\n" +
                "WHERE t1.id = ?\n" +
                "\tAND t1.tenant_id = 2", join1);

        String join2 = ASTDruidTestUtil.addAndCondition("select t1.a, t2.b from user t1 right join dept t2 on t1.dept_id = t2.id where t1.id = ?", "tenant_id = 2");
        Assert.assertEquals("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tRIGHT JOIN dept t2\n" +
                "\tON t1.dept_id = t2.id\n" +
                "\t\tAND t2.tenant_id = 2\n" +
                "WHERE t1.id = ?\n" +
                "\tAND t1.tenant_id = 2", join2);

        String join3 = ASTDruidTestUtil.addAndCondition("select t1.a, t2.b from user t1  join dept t2 on t1.dept_id = t2.id where t1.id = ?", "tenant_id = 2");
        Assert.assertEquals("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tJOIN dept t2\n" +
                "\tON t1.dept_id = t2.id\n" +
                "\t\tAND t2.tenant_id = 2\n" +
                "WHERE t1.id = ?\n" +
                "\tAND t1.tenant_id = 2", join3);

        // join 2个表
        String join4 = ASTDruidTestUtil.addAndCondition("select t1.a, t2.b from user t1 left join dept t2 on t1.dept_id = t2.id  left join dept t3 on t1.dept_id = t3.id  where t1.id = ?", "tenant_id = 2");
        Assert.assertEquals("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tLEFT JOIN dept t2\n" +
                "\tON t1.dept_id = t2.id\n" +
                "\t\tAND t2.tenant_id = 2\n" +
                "\tLEFT JOIN dept t3\n" +
                "\tON t1.dept_id = t3.id\n" +
                "\t\tAND t3.tenant_id = 2\n" +
                "WHERE t1.id = ?\n" +
                "\tAND t1.tenant_id = 2", join4);

        String join5 = ASTDruidTestUtil.addAndCondition("select t1.a, t2.b from user t1 right join dept t2 on t1.dept_id = t2.id right join dept t3 on t1.dept_id = t3.id where t1.id = ?", "tenant_id = 2");
        Assert.assertEquals("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tRIGHT JOIN dept t2\n" +
                "\tON t1.dept_id = t2.id\n" +
                "\t\tAND t2.tenant_id = 2\n" +
                "\tRIGHT JOIN dept t3\n" +
                "\tON t1.dept_id = t3.id\n" +
                "\t\tAND t3.tenant_id = 2\n" +
                "WHERE t1.id = ?\n" +
                "\tAND t1.tenant_id = 2", join5);

        String join6 = ASTDruidTestUtil.addAndCondition("select t1.a, t2.b from user t1 right join dept t2 on t1.dept_id = t2.id left join dept t3 on t1.dept_id = t3.id where t1.id = ?", "tenant_id = 2");
        Assert.assertEquals("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tRIGHT JOIN dept t2\n" +
                "\tON t1.dept_id = t2.id\n" +
                "\t\tAND t2.tenant_id = 2\n" +
                "\tLEFT JOIN dept t3\n" +
                "\tON t1.dept_id = t3.id\n" +
                "\t\tAND t3.tenant_id = 2\n" +
                "WHERE t1.id = ?\n" +
                "\tAND t1.tenant_id = 2", join6);

        String join7 = ASTDruidTestUtil.addAndCondition("select t1.a, t2.b from user t1 left join dept t2 on t1.dept_id = t2.id right join dept t3 on t1.dept_id = t3.id where t1.id = ?", "tenant_id = 2");
        Assert.assertEquals("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tLEFT JOIN dept t2\n" +
                "\tON t1.dept_id = t2.id\n" +
                "\t\tAND t2.tenant_id = 2\n" +
                "\tRIGHT JOIN dept t3\n" +
                "\tON t1.dept_id = t3.id\n" +
                "\t\tAND t3.tenant_id = 2\n" +
                "WHERE t1.id = ?\n" +
                "\tAND t1.tenant_id = 2", join7);

        String join8 = ASTDruidTestUtil.addAndCondition("select t1.a, t2.b from user t1  join dept t2 on t1.dept_id = t2.id  join dept t3 on t1.dept_id = t3.id where t1.id = ?", "tenant_id = 2");
        Assert.assertEquals("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tJOIN dept t2\n" +
                "\tON t1.dept_id = t2.id\n" +
                "\t\tAND t2.tenant_id = 2\n" +
                "\tJOIN dept t3\n" +
                "\tON t1.dept_id = t3.id\n" +
                "\t\tAND t3.tenant_id = 2\n" +
                "WHERE t1.id = ?\n" +
                "\tAND t1.tenant_id = 2", join8);

        String select1 = ASTDruidTestUtil.addAndCondition("select * from user  where id = ?", "tenant_id = 2");
        Assert.assertEquals("SELECT *\n" +
                "FROM user\n" +
                "WHERE id = ?\n" +
                "\tAND user.tenant_id = 2", select1);

        String select2 = ASTDruidTestUtil.addAndCondition("select * from user", "tenant_id = 2");
        Assert.assertEquals("SELECT *\n" +
                "FROM user\n" +
                "WHERE user.tenant_id = 2", select2);

        // union 联合
        String select3 = ASTDruidTestUtil.addAndCondition("select id,name from user t1 union select id,name from user", "tenant_id = 2");
        Assert.assertEquals("SELECT id, name\n" +
                "FROM user t1\n" +
                "WHERE t1.tenant_id = 2\n" +
                "UNION\n" +
                "SELECT id, name\n" +
                "FROM user\n" +
                "WHERE user.tenant_id = 2", select3);

        // 子查询
        String select4 = ASTDruidTestUtil.addAndCondition("select * from (select id,name from user t1 union select id,name from user) t ", "tenant_id = 2");
        Assert.assertEquals("SELECT *\n" +
                "FROM (\n" +
                "\tSELECT id, name\n" +
                "\tFROM user t1\n" +
                "\tWHERE t1.tenant_id = 2\n" +
                "\tUNION\n" +
                "\tSELECT id, name\n" +
                "\tFROM user\n" +
                "\tWHERE user.tenant_id = 2\n" +
                ") t", select4);

        String select5 = ASTDruidTestUtil.addAndCondition("select * from (select id,name from user t1 union all select id,name from user) t1 left join (select * from (select id,name from user t1 union all select id,name from user) t2) on t1.id = t2.id", "tenant_id = 2");
        Assert.assertEquals("SELECT *\n" +
                "FROM (\n" +
                "\tSELECT id, name\n" +
                "\tFROM user t1\n" +
                "\tWHERE t1.tenant_id = 2\n" +
                "\tUNION ALL\n" +
                "\tSELECT id, name\n" +
                "\tFROM user\n" +
                "\tWHERE user.tenant_id = 2\n" +
                ") t1\n" +
                "\tLEFT JOIN (\n" +
                "\t\tSELECT *\n" +
                "\t\tFROM (\n" +
                "\t\t\tSELECT id, name\n" +
                "\t\t\tFROM user t1\n" +
                "\t\t\tWHERE t1.tenant_id = 2\n" +
                "\t\t\tUNION ALL\n" +
                "\t\t\tSELECT id, name\n" +
                "\t\t\tFROM user\n" +
                "\t\t\tWHERE user.tenant_id = 2\n" +
                "\t\t) t2\n" +
                "\t)\n" +
                "\tON t1.id = t2.id", select5);

        String select6 = ASTDruidTestUtil.addAndCondition(
                "select * from " +
                        "(select id,name from user t1_1 union all select id,name from user t1_2) t1 " +
                        "left join (select * from (select id,name from user t2_1_1 union all select id,name from user t2_1_2) t2_1) t2 on t1.id = t2.id " +
                        "left join user t3 on t1.id = t3.id", "tenant_id = 2");
        Assert.assertEquals("SELECT *\n" +
                "FROM (\n" +
                "\tSELECT id, name\n" +
                "\tFROM user t1_1\n" +
                "\tWHERE t1_1.tenant_id = 2\n" +
                "\tUNION ALL\n" +
                "\tSELECT id, name\n" +
                "\tFROM user t1_2\n" +
                "\tWHERE t1_2.tenant_id = 2\n" +
                ") t1\n" +
                "\tLEFT JOIN (\n" +
                "\t\tSELECT *\n" +
                "\t\tFROM (\n" +
                "\t\t\tSELECT id, name\n" +
                "\t\t\tFROM user t2_1_1\n" +
                "\t\t\tWHERE t2_1_1.tenant_id = 2\n" +
                "\t\t\tUNION ALL\n" +
                "\t\t\tSELECT id, name\n" +
                "\t\t\tFROM user t2_1_2\n" +
                "\t\t\tWHERE t2_1_2.tenant_id = 2\n" +
                "\t\t) t2_1\n" +
                "\t) t2\n" +
                "\tON t1.id = t2.id\n" +
                "\tLEFT JOIN user t3\n" +
                "\tON t1.id = t3.id\n" +
                "\t\tAND t3.tenant_id = 2", select6);

        String select61 = ASTDruidTestUtil.addAndCondition(
                "select * from " +
                        "(select id,name from user t1_1 union all select id,name from user t1_2) t1 " +
                        "left join (select * from (select id,name from user t2_1_1 union all select id,name from user t2_1_2) t2_1) t2 on t1.id = t2.id " +
                        "left join user t3 on t1.id = t3.id " +
                        "where id = 1 ", "tenant_id = 2");
        Assert.assertEquals("SELECT *\n" +
                "FROM (\n" +
                "\tSELECT id, name\n" +
                "\tFROM user t1_1\n" +
                "\tWHERE t1_1.tenant_id = 2\n" +
                "\tUNION ALL\n" +
                "\tSELECT id, name\n" +
                "\tFROM user t1_2\n" +
                "\tWHERE t1_2.tenant_id = 2\n" +
                ") t1\n" +
                "\tLEFT JOIN (\n" +
                "\t\tSELECT *\n" +
                "\t\tFROM (\n" +
                "\t\t\tSELECT id, name\n" +
                "\t\t\tFROM user t2_1_1\n" +
                "\t\t\tWHERE t2_1_1.tenant_id = 2\n" +
                "\t\t\tUNION ALL\n" +
                "\t\t\tSELECT id, name\n" +
                "\t\t\tFROM user t2_1_2\n" +
                "\t\t\tWHERE t2_1_2.tenant_id = 2\n" +
                "\t\t) t2_1\n" +
                "\t) t2\n" +
                "\tON t1.id = t2.id\n" +
                "\tLEFT JOIN user t3\n" +
                "\tON t1.id = t3.id\n" +
                "\t\tAND t3.tenant_id = 2\n" +
                "WHERE id = 1", select61);

        String select7 = ASTDruidTestUtil.addAndCondition("select * from user left join dept on dept.id = user.dept_id", "tenant_id = 2");
        Assert.assertEquals("SELECT *\n" +
                "FROM user\n" +
                "\tLEFT JOIN dept\n" +
                "\tON dept.id = user.dept_id\n" +
                "\t\tAND dept.tenant_id = 2\n" +
                "WHERE user.tenant_id = 2", select7);

        String select8 = ASTDruidTestUtil.addAndCondition("select * from user t1 left join dept on dept.id = t1.dept_id", "tenant_id = 2");
        Assert.assertEquals("SELECT *\n" +
                "FROM user t1\n" +
                "\tLEFT JOIN dept\n" +
                "\tON dept.id = t1.dept_id\n" +
                "\t\tAND dept.tenant_id = 2\n" +
                "WHERE t1.tenant_id = 2", select8);

        String select9 = ASTDruidTestUtil.addAndCondition("select * from user  left join dept t1 on t1.id = user.dept_id", "tenant_id = 2");
        Assert.assertEquals("SELECT *\n" +
                "FROM user\n" +
                "\tLEFT JOIN dept t1\n" +
                "\tON t1.id = user.dept_id\n" +
                "\t\tAND t1.tenant_id = 2\n" +
                "WHERE user.tenant_id = 2", select9);

        // or
        String select10 = ASTDruidTestUtil.addAndCondition("select * from user  where id = 1 or name = '1' ", "tenant_id = 2");
        Assert.assertEquals("SELECT *\n" +
                "FROM user\n" +
                "WHERE (id = 1\n" +
                "\t\tOR name = '1')\n" +
                "\tAND user.tenant_id = 2", select10);

        String select11 = ASTDruidTestUtil.addAndCondition("select * from user  where (id = 1 or name = '1') and (id = 2 or name = '2') ", "tenant_id = 2");
        Assert.assertEquals("SELECT *\n" +
                "FROM user\n" +
                "WHERE (id = 1\n" +
                "\t\tOR name = '1')\n" +
                "\tAND (id = 2\n" +
                "\t\tOR name = '2')\n" +
                "\tAND user.tenant_id = 2", select11);

        String select12 = ASTDruidTestUtil.addAndCondition("select * from user  where (id = 1 and name = '1') or (id = 2 and name = '2') ", "tenant_id = 2");
        Assert.assertEquals("SELECT *\n" +
                "FROM user\n" +
                "WHERE ((id = 1\n" +
                "\t\t\tAND name = '1')\n" +
                "\t\tOR (id = 2\n" +
                "\t\t\tAND name = '2'))\n" +
                "\tAND user.tenant_id = 2", select12);

        // EXISTS
        String select13 = ASTDruidTestUtil.addAndCondition("select * from user  where (id = 1 and name = '1') or (id = 2 and name = '2')  and EXISTS (select id from p where p.id = id)", "tenant_id = 2");
        Assert.assertEquals("SELECT *\n" +
                "FROM user\n" +
                "WHERE ((id = 1\n" +
                "\t\t\tAND name = '1')\n" +
                "\t\tOR id = 2\n" +
                "\t\tAND name = '2'\n" +
                "\t\tAND EXISTS (\n" +
                "\t\t\tSELECT id\n" +
                "\t\t\tFROM p\n" +
                "\t\t\tWHERE p.id = id\n" +
                "\t\t\t\tAND p.tenant_id = 2\n" +
                "\t\t))\n" +
                "\tAND user.tenant_id = 2", select13);

        String select14 = ASTDruidTestUtil.addAndCondition("select * from user  where (id = 1 and name = '1') or (id = 2 and name = '2')  and EXISTS (select id from p1 left join p2 on p2.id = p1.dept_id where p1.id = user.id)", "tenant_id = 2");
        Assert.assertEquals("SELECT *\n" +
                "FROM user\n" +
                "WHERE ((id = 1\n" +
                "\t\t\tAND name = '1')\n" +
                "\t\tOR id = 2\n" +
                "\t\tAND name = '2'\n" +
                "\t\tAND EXISTS (\n" +
                "\t\t\tSELECT id\n" +
                "\t\t\tFROM p1\n" +
                "\t\t\t\tLEFT JOIN p2\n" +
                "\t\t\t\tON p2.id = p1.dept_id\n" +
                "\t\t\t\t\tAND p2.tenant_id = 2\n" +
                "\t\t\tWHERE p1.id = user.id\n" +
                "\t\t\t\tAND p1.tenant_id = 2\n" +
                "\t\t))\n" +
                "\tAND user.tenant_id = 2", select14);

        String select15 = ASTDruidTestUtil.addAndCondition("select case when EXISTS (select id from p1 left join p2 on p2.id = p1.dept_id where p1.id = user.id) then 1 end from user  where (id = 1 and name = '1') or (id = 2 and name = '2')  " +
                "and EXISTS (select id from p1 left join p2 on p2.id = p1.dept_id where p1.id = user.id)", "tenant_id = 2");
        Assert.assertEquals("SELECT CASE \n" +
                "\t\tWHEN EXISTS (\n" +
                "\t\t\tSELECT id\n" +
                "\t\t\tFROM p1\n" +
                "\t\t\t\tLEFT JOIN p2\n" +
                "\t\t\t\tON p2.id = p1.dept_id\n" +
                "\t\t\t\t\tAND p2.tenant_id = 2\n" +
                "\t\t\tWHERE p1.id = user.id\n" +
                "\t\t\t\tAND p1.tenant_id = 2\n" +
                "\t\t) THEN 1\n" +
                "\tEND\n" +
                "FROM user\n" +
                "WHERE ((id = 1\n" +
                "\t\t\tAND name = '1')\n" +
                "\t\tOR id = 2\n" +
                "\t\tAND name = '2'\n" +
                "\t\tAND EXISTS (\n" +
                "\t\t\tSELECT id\n" +
                "\t\t\tFROM p1\n" +
                "\t\t\t\tLEFT JOIN p2\n" +
                "\t\t\t\tON p2.id = p1.dept_id\n" +
                "\t\t\t\t\tAND p2.tenant_id = 2\n" +
                "\t\t\tWHERE p1.id = user.id\n" +
                "\t\t\t\tAND p1.tenant_id = 2\n" +
                "\t\t))\n" +
                "\tAND user.tenant_id = 2", select15);

        String select16 = ASTDruidTestUtil.addAndCondition("select (select count(1) from p3 where p3.id = user.id) from user  where (id = 1 and name = '1') or (id = 2 and name = '2')  " +
                "and EXISTS (select id from p1 left join p2 on p2.id = p1.dept_id where p1.id = user.id)", "tenant_id = 2");
        Assert.assertEquals("SELECT (\n" +
                "\t\tSELECT count(1)\n" +
                "\t\tFROM p3\n" +
                "\t\tWHERE p3.id = user.id\n" +
                "\t\t\tAND p3.tenant_id = 2\n" +
                "\t)\n" +
                "FROM user\n" +
                "WHERE ((id = 1\n" +
                "\t\t\tAND name = '1')\n" +
                "\t\tOR id = 2\n" +
                "\t\tAND name = '2'\n" +
                "\t\tAND EXISTS (\n" +
                "\t\t\tSELECT id\n" +
                "\t\t\tFROM p1\n" +
                "\t\t\t\tLEFT JOIN p2\n" +
                "\t\t\t\tON p2.id = p1.dept_id\n" +
                "\t\t\t\t\tAND p2.tenant_id = 2\n" +
                "\t\t\tWHERE p1.id = user.id\n" +
                "\t\t\t\tAND p1.tenant_id = 2\n" +
                "\t\t))\n" +
                "\tAND user.tenant_id = 2", select16);

        // case WHEN END
        String casewhen = ASTDruidTestUtil.addAndCondition("SELECT\n" +
                "\tt10.interview_operate_uid AS interviewOperateUid,\n" +
                "\tt10.over_status AS overStatus,\n" +
                "\tt11.enter_offer_time AS enterOfferTime,\n" +
                "\tt10.enterprise_pass_time AS enterprisePassTime,\n" +
                "\tt10.add_time AS addtime,\n" +
                "\tt10.guarantee_time AS guaranteeTime,\n" +
                "\tt10.upload_time AS uploadTime,\n" +
                "\tt10.query_status AS queryStatus,\n" +
                "\tt10.sub_query_status AS subQueryStatus,\n" +
                "\tt.sign_time AS signTime,\n" +
                "\tt10.estimate_time AS estimateTime,\n" +
                "\tt10.last_interview_id AS lastInterviewId,\n" +
                "\tt10.last_offer_id AS offerId,\n" +
                "\tt10.last_entry_id AS entryId,\n" +
                "\tt10.probation_time AS probationTime,\n" +
                "\tt10.entry_time AS entryTime,\n" +
                "\tt10.entry_operate_time AS entryOperateTime,\n" +
                "\tt10.invoice_status AS invoiceStatus,\n" +
                "\tt.last_remark AS remarks,\n" +
                "\tt10.invoice_time AS invoiceTime,\n" +
                "\tdate_format( t10.invoice_time, '%Y-%m-%d' ) AS invoiceTimeStr,\n" +
                "\tifnull( t10.no_invoice_event, 10 ) AS noInvoiceEvent,\n" +
                "\tt.id AS pipelineid,\n" +
                "\tt.offer_rank AS offerRank,\n" +
                "\tt3.brand,\n" +
                "\tt3.NAME AS companyName,\n" +
                "\tt4.NAME AS baseNodeName,\n" +
                "\tt1.NAME AS positionName,\n" +
                "\tt1.city_id AS cityId,\n" +
                "\tt8.`talent_name` AS talentName,\n" +
                "\tt8.`id` AS talentId,\n" +
                "\tt8.`now_base_company_id` AS nowBaseCompanyId,\n" +
                "\tt8.`now_base_company_name` AS nowBaseCompanyName,\n" +
                "\tt8.now_func_type_name AS nowBasePositionName,\n" +
                "\tt8.base_func_type_ids AS baseFuncTypeIds,\n" +
                "\tt8.mobile AS phone,\n" +
                "\tt.`last_rec_time` AS recTime,\n" +
                "\tt.pipeline_state AS pipelineState,\n" +
                "\tt.current_stage AS currentStage,\n" +
                "\tt.confirm_type AS confirmType,\n" +
                "\tt.feed_back_type AS feedBackType,\n" +
                "\tt.create_uid AS createUid,\n" +
                "\tt1.STATUS,\n" +
                "CASE\n" +
                "\t\t\n" +
                "\t\tWHEN t.pipeline_state = 110 THEN\n" +
                "\t\t'成功' \n" +
                "\t\tWHEN t.pipeline_state = 120 THEN\n" +
                "\t\t'失败' ELSE '流程中' \n" +
                "\tEND AS talentStatus,\n" +
                "CASE\n" +
                "\t\t\n" +
                "\t\tWHEN t.state_and_operate != 120 \n" +
                "\t\tAND t.state_and_operate != 100 THEN\n" +
                "\t\t\tt.state_and_operate ELSE\n" +
                "\t\tCASE\n" +
                "\t\t\t\t\n" +
                "\t\t\t\tWHEN t.last_interview_time < now() THEN\n" +
                "\t\t\t\t120 ELSE 100 \n" +
                "\t\t\tEND \n" +
                "\t\t\tEND AS stateAndOperate,\n" +
                "\t\t\tt.`last_update_time` AS lastUpdateTime,\n" +
                "\t\t\tt.pm_uid AS pmUid,\n" +
                "\t\t\tt.`consultant_id` AS recUid,\n" +
                "\t\t\tt.`last_interview_time` AS lastInterviewTime,\n" +
                "\t\t\tt1.contact_name AS contactName,\n" +
                "\t\t\tt.last_operate_state AS eventCode,\n" +
                "\t\t\tt.biz_corp_id AS companyId,\n" +
                "\t\t\tt.biz_position_id AS positionId,\n" +
                "\t\t\tt.interview_flag AS interviewFlag,\n" +
                "\t\t\tt.step AS step \n" +
                "\t\tFROM\n" +
                "\t\t\tpipeline t\n" +
                "\t\t\tINNER JOIN pipeline_ext t10 ON t10.pipeline_id = t.id \n" +
                "\t\t\tAND t10.tenant_id = 1\n" +
                "\t\t\tLEFT JOIN biz_position t1 ON t.biz_position_id = t1.id \n" +
                "\t\t\tAND t1.tenant_id = 1\n" +
                "\t\t\tLEFT JOIN ( SELECT min( create_time ) AS enter_offer_time, pipeline_id FROM pipeline_offer offer WHERE offer.delete_flag = 0 AND offer.tenant_id = 1 GROUP BY pipeline_id ) t11 ON t11.pipeline_id = t.id\n" +
                "\t\t\tLEFT JOIN biz_corp t3 ON t3.id = t.biz_corp_id \n" +
                "\t\t\tAND t3.tenant_id = 1\n" +
                "\t\t\tLEFT JOIN base_node t4 ON t4.id = t1.base_node_id \n" +
                "\t\t\tAND t4.tenant_id = 1\n" +
                "\t\t\tLEFT JOIN talent t8 ON t8.id = t.talent_id \n" +
                "\t\t\tAND t8.tenant_id = 1 \n" +
                "\t\tWHERE\n" +
                "\t\t\tt.delete_flag = 0 \n" +
                "\t\t\tAND (\n" +
                "\t\t\t\tt10.over_status = ? \n" +
                "\t\t\tOR t.state_and_operate IN ( 10, 40 )) \n" +
                "\t\tAND\n" +
                "\t\tCASE\n" +
                "\t\t\t\t\n" +
                "\t\t\t\tWHEN t.state_and_operate != 120 \n" +
                "\t\t\t\tAND t.state_and_operate != 100 THEN\n" +
                "\t\t\t\t\tt.state_and_operate ELSE\n" +
                "\t\t\t\tCASE\n" +
                "\t\t\t\t\t\t\n" +
                "\t\t\t\t\t\tWHEN t.last_interview_time < now() THEN\n" +
                "\t\t\t\t\t\t120 ELSE 100 \n" +
                "\t\t\t\t\tEND \n" +
                "\t\t\t\t\t\tEND IN (?,\n" +
                "\t\t\t\t\t\t?,\n" +
                "\t\t\t\t\t\t?,\n" +
                "\t\t\t\t\t?) \n" +
                "\t\t\t\t\tAND t.consultant_id IN (?) \n" +
                "\t\t\t\t\tAND 1 = 1 \n" +
                "\t\t\t\t\tAND ( t.last_rec_time BETWEEN ? AND ? OR t.state_and_operate = 10 ) \n" +
                "\t\t\t\t\tAND t.tenant_id = 1 \n" +
                "\t\t\t\tORDER BY\n" +
                "\t\t\t\tt.`last_update_time` DESC \n" +
                "\tLIMIT ?", "tenant_id = 2");

        Assert.assertEquals("SELECT\n" +
                "\tt10.interview_operate_uid AS interviewOperateUid,\n" +
                "\tt10.over_status AS overStatus,\n" +
                "\tt11.enter_offer_time AS enterOfferTime,\n" +
                "\tt10.enterprise_pass_time AS enterprisePassTime,\n" +
                "\tt10.add_time AS addtime,\n" +
                "\tt10.guarantee_time AS guaranteeTime,\n" +
                "\tt10.upload_time AS uploadTime,\n" +
                "\tt10.query_status AS queryStatus,\n" +
                "\tt10.sub_query_status AS subQueryStatus,\n" +
                "\tt.sign_time AS signTime,\n" +
                "\tt10.estimate_time AS estimateTime,\n" +
                "\tt10.last_interview_id AS lastInterviewId,\n" +
                "\tt10.last_offer_id AS offerId,\n" +
                "\tt10.last_entry_id AS entryId,\n" +
                "\tt10.probation_time AS probationTime,\n" +
                "\tt10.entry_time AS entryTime,\n" +
                "\tt10.entry_operate_time AS entryOperateTime,\n" +
                "\tt10.invoice_status AS invoiceStatus,\n" +
                "\tt.last_remark AS remarks,\n" +
                "\tt10.invoice_time AS invoiceTime,\n" +
                "\tdate_format( t10.invoice_time, '%Y-%m-%d' ) AS invoiceTimeStr,\n" +
                "\tifnull( t10.no_invoice_event, 10 ) AS noInvoiceEvent,\n" +
                "\tt.id AS pipelineid,\n" +
                "\tt.offer_rank AS offerRank,\n" +
                "\tt3.brand,\n" +
                "\tt3.NAME AS companyName,\n" +
                "\tt4.NAME AS baseNodeName,\n" +
                "\tt1.NAME AS positionName,\n" +
                "\tt1.city_id AS cityId,\n" +
                "\tt8.`talent_name` AS talentName,\n" +
                "\tt8.`id` AS talentId,\n" +
                "\tt8.`now_base_company_id` AS nowBaseCompanyId,\n" +
                "\tt8.`now_base_company_name` AS nowBaseCompanyName,\n" +
                "\tt8.now_func_type_name AS nowBasePositionName,\n" +
                "\tt8.base_func_type_ids AS baseFuncTypeIds,\n" +
                "\tt8.mobile AS phone,\n" +
                "\tt.`last_rec_time` AS recTime,\n" +
                "\tt.pipeline_state AS pipelineState,\n" +
                "\tt.current_stage AS currentStage,\n" +
                "\tt.confirm_type AS confirmType,\n" +
                "\tt.feed_back_type AS feedBackType,\n" +
                "\tt.create_uid AS createUid,\n" +
                "\tt1.STATUS,\n" +
                "CASE\n" +
                "\t\t\n" +
                "\t\tWHEN t.pipeline_state = 110 THEN\n" +
                "\t\t'成功' \n" +
                "\t\tWHEN t.pipeline_state = 120 THEN\n" +
                "\t\t'失败' ELSE '流程中' \n" +
                "\tEND AS talentStatus,\n" +
                "CASE\n" +
                "\t\t\n" +
                "\t\tWHEN t.state_and_operate != 120 \n" +
                "\t\tAND t.state_and_operate != 100 THEN\n" +
                "\t\t\tt.state_and_operate ELSE\n" +
                "\t\tCASE\n" +
                "\t\t\t\t\n" +
                "\t\t\t\tWHEN t.last_interview_time < now() THEN\n" +
                "\t\t\t\t120 ELSE 100 \n" +
                "\t\t\tEND \n" +
                "\t\t\tEND AS stateAndOperate,\n" +
                "\t\t\tt.`last_update_time` AS lastUpdateTime,\n" +
                "\t\t\tt.pm_uid AS pmUid,\n" +
                "\t\t\tt.`consultant_id` AS recUid,\n" +
                "\t\t\tt.`last_interview_time` AS lastInterviewTime,\n" +
                "\t\t\tt1.contact_name AS contactName,\n" +
                "\t\t\tt.last_operate_state AS eventCode,\n" +
                "\t\t\tt.biz_corp_id AS companyId,\n" +
                "\t\t\tt.biz_position_id AS positionId,\n" +
                "\t\t\tt.interview_flag AS interviewFlag,\n" +
                "\t\t\tt.step AS step \n" +
                "\t\tFROM\n" +
                "\t\t\tpipeline t\n" +
                "\t\t\tINNER JOIN pipeline_ext t10 ON t10.pipeline_id = t.id \n" +
                "\t\t\tAND t10.tenant_id = 1\n" +
                "\t\t\tLEFT JOIN biz_position t1 ON t.biz_position_id = t1.id \n" +
                "\t\t\tAND t1.tenant_id = 1\n" +
                "\t\t\tLEFT JOIN ( SELECT min( create_time ) AS enter_offer_time, pipeline_id FROM pipeline_offer offer WHERE offer.delete_flag = 0 AND offer.tenant_id = 1 GROUP BY pipeline_id ) t11 ON t11.pipeline_id = t.id\n" +
                "\t\t\tLEFT JOIN biz_corp t3 ON t3.id = t.biz_corp_id \n" +
                "\t\t\tAND t3.tenant_id = 1\n" +
                "\t\t\tLEFT JOIN base_node t4 ON t4.id = t1.base_node_id \n" +
                "\t\t\tAND t4.tenant_id = 1\n" +
                "\t\t\tLEFT JOIN talent t8 ON t8.id = t.talent_id \n" +
                "\t\t\tAND t8.tenant_id = 1 \n" +
                "\t\tWHERE\n" +
                "\t\t\tt.delete_flag = 0 \n" +
                "\t\t\tAND (\n" +
                "\t\t\t\tt10.over_status = ? \n" +
                "\t\t\tOR t.state_and_operate IN ( 10, 40 )) \n" +
                "\t\tAND\n" +
                "\t\tCASE\n" +
                "\t\t\t\t\n" +
                "\t\t\t\tWHEN t.state_and_operate != 120 \n" +
                "\t\t\t\tAND t.state_and_operate != 100 THEN\n" +
                "\t\t\t\t\tt.state_and_operate ELSE\n" +
                "\t\t\t\tCASE\n" +
                "\t\t\t\t\t\t\n" +
                "\t\t\t\t\t\tWHEN t.last_interview_time < now() THEN\n" +
                "\t\t\t\t\t\t120 ELSE 100 \n" +
                "\t\t\t\t\tEND \n" +
                "\t\t\t\t\t\tEND IN (?,\n" +
                "\t\t\t\t\t\t?,\n" +
                "\t\t\t\t\t\t?,\n" +
                "\t\t\t\t\t?) \n" +
                "\t\t\t\t\tAND t.consultant_id IN (?) \n" +
                "\t\t\t\t\tAND 1 = 1 \n" +
                "\t\t\t\t\tAND ( t.last_rec_time BETWEEN ? AND ? OR t.state_and_operate = 10 ) \n" +
                "\t\t\t\t\tAND t.tenant_id = 1 \n" +
                "\t\t\t\tORDER BY\n" +
                "\t\t\t\tt.`last_update_time` DESC \n" +
                "\tLIMIT ?", casewhen);

        // @
        String select17 = ASTDruidTestUtil.addAndCondition("SELECT\n" +
                "* \n" +
                "FROM\n" +
                "\t(\n" +
                "\tSELECT\n" +
                "\t\ttt.*,\n" +
                "\tCASE\n" +
                "\t\t\t\n" +
                "\t\t\tWHEN @rowtotal = tt.DATA THEN\n" +
                "\t\t\t@rownum \n" +
                "\t\t\tWHEN @rowtotal := tt.DATA THEN\n" +
                "\t\t\t@rownum := @rownum + 1 \n" +
                "\t\t\tWHEN @rowtotal = 0 THEN\n" +
                "\t\t\t@rownum := @rownum + 1 \n" +
                "\t\tEND AS rank \n" +
                "\tFROM\n" +
                "\t\t( SELECT @rownum := 0, @rowtotal := NULL ) r,\n" +
                "\t\t(\n" +
                "\t\tSELECT\n" +
                "\t\t\tpu.name_en AS pUserName,\n" +
                "\t\t\tpu.head_pic AS headPortrait,\n" +
                "\t\t\tpu.dept_id AS deptId,\n" +
                "\t\t\tt.p_user_id AS pUserId,\n" +
                "\t\t\tSUM( t.money ) AS DATA \n" +
                "\t\tFROM\n" +
                "\t\t\t(\n" +
                "\t\t\tSELECT DISTINCT \n" +
                "\t\t\t\tda.id AS id,\n" +
                "\t\t\t\tda.p_user_id AS p_user_id,\n" +
                "\t\t\t\tda.after_tax_money AS money \n" +
                "\t\t\tFROM\n" +
                "\t\t\t\tdata_achieve da\n" +
                "\t\t\t\tLEFT JOIN data_achieve_user_role dar ON da.id = dar.data_achieve_id \n" +
                "\t\t\t\tAND dar.`delete_flag` = 0\n" +
                "\t\t\t\tLEFT JOIN pipeline_invoice pi ON da.invoice_id = pi.id \n" +
                "\t\t\tWHERE\n" +
                "\t\t\t\tda.delete_flag = 0 \n" +
                "\t\t\t\tAND pi.delete_flag = 0 \n" +
                "\t\t\t\tAND pi.`status` != 270 \n" +
                "\t\t\t) t\n" +
                "\t\t\tLEFT JOIN p_user pu ON t.p_user_id = pu.id\n" +
                "\t\t\tLEFT JOIN (\n" +
                "\t\t\tSELECT DISTINCT\n" +
                "\t\t\t\t( oid ) AS oid \n" +
                "\t\t\tFROM\n" +
                "\t\t\t\tp_role_acl pra1\n" +
                "\t\t\t\tLEFT JOIN p_role pr1 ON pra1.rid = pr1.id \n" +
                "\t\t\tWHERE\n" +
                "\t\t\t\tpra1.is_delete = 0 \n" +
                "\t\t\t\tAND pra1.type = 0 \n" +
                "\t\t\t) t1 ON pu.id = t1.oid \n" +
                "\t\tWHERE\n" +
                "\t\t\tpu.STATUS = 0 \n" +
                "\t\t\tAND t1.oid IS NOT NULL \n" +
                "\t\tGROUP BY\n" +
                "\t\t\tt.p_user_id \n" +
                "\t\tORDER BY\n" +
                "\t\tDATA DESC \n" +
                "\t\t) AS tt \n" +
                "\tWHERE\n" +
                "\t\ttt.DATA > 0 \n" +
                "\t) b \n" +
                "\tLIMIT 10", "tenant_id = 2");
        Assert.assertEquals("SELECT *\n" +
                "FROM (\n" +
                "\tSELECT tt.*\n" +
                "\t\t, CASE \n" +
                "\t\t\tWHEN @rowtotal = tt.DATA THEN @rownum\n" +
                "\t\t\tWHEN @rowtotal := tt.DATA THEN @rownum := @rownum + 1\n" +
                "\t\t\tWHEN @rowtotal = 0 THEN @rownum := @rownum + 1\n" +
                "\t\tEND AS rank\n" +
                "\tFROM (\n" +
                "\t\tSELECT @rownum := 0, @rowtotal := NULL\n" +
                "\t) r, (\n" +
                "\t\t\tSELECT pu.name_en AS pUserName, pu.head_pic AS headPortrait, pu.dept_id AS deptId, t.p_user_id AS pUserId\n" +
                "\t\t\t\t, SUM(t.money) AS DATA\n" +
                "\t\t\tFROM (\n" +
                "\t\t\t\tSELECT DISTINCT da.id AS id, da.p_user_id AS p_user_id, da.after_tax_money AS money\n" +
                "\t\t\t\tFROM data_achieve da\n" +
                "\t\t\t\t\tLEFT JOIN data_achieve_user_role dar\n" +
                "\t\t\t\t\tON da.id = dar.data_achieve_id\n" +
                "\t\t\t\t\t\tAND dar.`delete_flag` = 0\n" +
                "\t\t\t\t\t\tAND dar.tenant_id = 2\n" +
                "\t\t\t\t\tLEFT JOIN pipeline_invoice pi\n" +
                "\t\t\t\t\tON da.invoice_id = pi.id\n" +
                "\t\t\t\t\t\tAND pi.tenant_id = 2\n" +
                "\t\t\t\tWHERE da.delete_flag = 0\n" +
                "\t\t\t\t\tAND pi.delete_flag = 0\n" +
                "\t\t\t\t\tAND pi.`status` != 270\n" +
                "\t\t\t\t\tAND da.tenant_id = 2\n" +
                "\t\t\t) t\n" +
                "\t\t\t\tLEFT JOIN p_user pu\n" +
                "\t\t\t\tON t.p_user_id = pu.id\n" +
                "\t\t\t\t\tAND pu.tenant_id = 2\n" +
                "\t\t\t\tLEFT JOIN (\n" +
                "\t\t\t\t\tSELECT DISTINCT oid AS oid\n" +
                "\t\t\t\t\tFROM p_role_acl pra1\n" +
                "\t\t\t\t\t\tLEFT JOIN p_role pr1\n" +
                "\t\t\t\t\t\tON pra1.rid = pr1.id\n" +
                "\t\t\t\t\t\t\tAND pr1.tenant_id = 2\n" +
                "\t\t\t\t\tWHERE pra1.is_delete = 0\n" +
                "\t\t\t\t\t\tAND pra1.type = 0\n" +
                "\t\t\t\t\t\tAND pra1.tenant_id = 2\n" +
                "\t\t\t\t) t1\n" +
                "\t\t\t\tON pu.id = t1.oid\n" +
                "\t\t\tWHERE pu.STATUS = 0\n" +
                "\t\t\t\tAND t1.oid IS NOT NULL\n" +
                "\t\t\tGROUP BY t.p_user_id\n" +
                "\t\t\tORDER BY DATA DESC\n" +
                "\t\t) tt\n" +
                "\tWHERE tt.DATA > 0\n" +
                ") b\n" +
                "LIMIT 10", select17);

        System.out.println("select");
    }

    @Test
    public void update() {
        String updateexist = ASTDruidTestUtil.addAndCondition(" UPDATE user  SET `status` = 2 WHERE id =1 and tenant_id = 1", "tenant_id = 2");
        Assert.assertEquals(" UPDATE user  SET `status` = 2 WHERE id =1 and tenant_id = 1", updateexist);

        String join = ASTDruidTestUtil.addAndCondition(" update pipeline\n" +
                "      left join (select interview_time, pipeline_id from `pipeline_interview`\n" +
                "      where delete_flag = 0 and cancel_interview_flag = 0 and pipeline_id = 1\n" +
                "      order by create_time desc,id desc limit 1) t2 on t2.pipeline_id = pipeline.id\n" +
                "      set pipeline.last_interview_time = t2.interview_time where pipeline.id = 1", "tenant_id = 2");
        Assert.assertEquals("UPDATE pipeline\n" +
                "\tLEFT JOIN (\n" +
                "\t\tSELECT interview_time, pipeline_id\n" +
                "\t\tFROM `pipeline_interview`\n" +
                "\t\tWHERE delete_flag = 0\n" +
                "\t\t\tAND cancel_interview_flag = 0\n" +
                "\t\t\tAND pipeline_id = 1\n" +
                "\t\t\tAND pipeline_interview.tenant_id = 2\n" +
                "\t\tORDER BY create_time DESC, id DESC\n" +
                "\t\tLIMIT 1\n" +
                "\t) t2\n" +
                "\tON t2.pipeline_id = pipeline.id\n" +
                "SET pipeline.last_interview_time = t2.interview_time\n" +
                "WHERE pipeline.id = 1\n" +
                "\tAND pipeline.tenant_id = 2", join);

        String update1 = ASTDruidTestUtil.addAndCondition(" UPDATE user t1 SET `status` = 0 WHERE id = 1 or name = 2", "tenant_id = 2");
        Assert.assertEquals("UPDATE user t1\n" +
                "SET `status` = 0\n" +
                "WHERE (id = 1\n" +
                "\t\tOR name = 2)\n" +
                "\tAND t1.tenant_id = 2", update1);

        String update2 = ASTDruidTestUtil.addAndCondition(" UPDATE user t1, dept t2  SET t1.`status` = t2.status WHERE t1.dept_id = t2.id", "tenant_id = 2");
        Assert.assertEquals("UPDATE user t1, dept t2\n" +
                "SET t1.`status` = t2.status\n" +
                "WHERE t1.dept_id = t2.id\n" +
                "\tAND t1.tenant_id = 2\n" +
                "\tAND t2.tenant_id = 2", update2);

        String update3 = ASTDruidTestUtil.addAndCondition(" UPDATE user t1, dept t2  SET t1.`status` = t2.status WHERE t1.dept_id = t2.id and t1.id in (select id from user)", "tenant_id = 2");
        Assert.assertEquals("UPDATE user t1, dept t2\n" +
                "SET t1.`status` = t2.status\n" +
                "WHERE t1.dept_id = t2.id\n" +
                "\tAND t1.id IN (\n" +
                "\t\tSELECT id\n" +
                "\t\tFROM user\n" +
                "\t\tWHERE user.tenant_id = 2\n" +
                "\t)\n" +
                "\tAND t1.tenant_id = 2\n" +
                "\tAND t2.tenant_id = 2", update3);

        System.out.println("update");
    }

    @Test
    public void insert() {
        String insert1 = ASTDruidTestUtil.addAndCondition("insert into `base_area` (`id`, `name`) select id,name from copy ", "tenant_id = 2");
        Assert.assertEquals("INSERT INTO `base_area` (`id`, `name`)\n" +
                "SELECT id, name\n" +
                "FROM copy\n" +
                "WHERE copy.tenant_id = 2", insert1);

        String insert2 = ASTDruidTestUtil.addAndCondition("replace into `base_area` (`id`, `name`) select id,name from copy ", "tenant_id = 2");
        Assert.assertEquals("REPLACE INTO `base_area` (`id`, `name`)\n" +
                "\tSELECT id, name\n" +
                "\tFROM copy\n" +
                "\tWHERE copy.tenant_id = 2", insert2);

        String insert3 = ASTDruidTestUtil.addAndCondition("select * into p_user_1 FROM p_user  ", "tenant_id = 2");
        Assert.assertEquals("SELECT *\n" +
                "INTO p_user_1\n" +
                "FROM p_user\n" +
                "WHERE p_user.tenant_id = 2", insert3);

        System.out.println("insert");
    }

    @Test
    public void delete() {
        String delete1 = ASTDruidTestUtil.addAndCondition("delete from user", "tenant_id = 2");
        Assert.assertEquals("DELETE FROM user\n" +
                "WHERE user.tenant_id = 2", delete1);

        String delete2 = ASTDruidTestUtil.addAndCondition(" delete from user t1  WHERE id = 1 or name = 2", "tenant_id = 2");
        Assert.assertEquals("DELETE FROM user t1\n" +
                "WHERE (id = 1\n" +
                "\t\tOR name = 2)\n" +
                "\tAND t1.tenant_id = 2", delete2);

        String delete3 = ASTDruidTestUtil.addAndCondition(" delete from user t1, dept t2 WHERE t1.dept_id = t2.id", "tenant_id = 2");
        Assert.assertEquals("DELETE FROM user t1, dept t2\n" +
                "WHERE t1.dept_id = t2.id\n" +
                "\tAND t1.tenant_id = 2\n" +
                "\tAND t2.tenant_id = 2", delete3);

        String delete4 = ASTDruidTestUtil.addAndCondition(" delete from user t1, dept t2 WHERE t1.dept_id = t2.id and t1.id in (select id from user)", "tenant_id = 2");
        Assert.assertEquals("DELETE FROM user t1, dept t2\n" +
                "WHERE t1.dept_id = t2.id\n" +
                "\tAND t1.id IN (\n" +
                "\t\tSELECT id\n" +
                "\t\tFROM user\n" +
                "\t\tWHERE user.tenant_id = 2\n" +
                "\t)\n" +
                "\tAND t1.tenant_id = 2\n" +
                "\tAND t2.tenant_id = 2", delete4);

        System.out.println("delete");
    }
}
