package com.github.mybatisintercept;


import com.github.mybatisintercept.util.ASTDruidUtil;
import org.junit.Assert;

public class ASTDruidConditionUtilTest {

    public static void main(String[] args) {
        test();
        select();
        update();
        delete();
        insert();
        System.out.println();
    }

    private static void test() {
        System.out.println("test");
    }

    private static void select() {
        String insert3 = ASTDruidUtil.addAndCondition("SELECT * INTO p_user_1 FROM p_user  ", "tenant_id = 2", "mysql");
        Assert.assertEquals("SELECT *\n" +
                "INTO p_user_1\n" +
                "FROM p_user\n" +
                "WHERE p_user.tenant_id = 2", insert3);

        String set = ASTDruidUtil.addAndCondition("set global query_cache_type = OFF ", "tenant_id = 2", "mysql");
        Assert.assertEquals("set global query_cache_type = OFF ", set);

        String at = ASTDruidUtil.addAndCondition("SELECT @@auto_generate_certs ", "tenant_id = 2", "mysql");
        Assert.assertEquals("SELECT @@auto_generate_certs ", at);

        String show = ASTDruidUtil.addAndCondition("SHOW VARIABLES ", "tenant_id = 2", "mysql");
        Assert.assertEquals("SHOW VARIABLES ", show);

        String dual = ASTDruidUtil.addAndCondition("SELECT 1 FROM DUAL ", "tenant_id = 2", "mysql");
        Assert.assertEquals("SELECT 1 FROM DUAL ", dual);

        // join 1个表
        String join1 = ASTDruidUtil.addAndCondition("select t1.a, t2.b from user t1 left join dept t2 on t1.dept_id = t2.id where t1.id = ?", "tenant_id = 2", "mysql");
        Assert.assertEquals("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tLEFT JOIN dept t2\n" +
                "\tON t1.dept_id = t2.id\n" +
                "\t\tAND t2.tenant_id = 2\n" +
                "WHERE t1.id = ?\n" +
                "\tAND t1.tenant_id = 2", join1);

        String join2 = ASTDruidUtil.addAndCondition("select t1.a, t2.b from user t1 right join dept t2 on t1.dept_id = t2.id where t1.id = ?", "tenant_id = 2", "mysql");
        Assert.assertEquals("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tRIGHT JOIN dept t2\n" +
                "\tON t1.dept_id = t2.id\n" +
                "\t\tAND t2.tenant_id = 2\n" +
                "WHERE t1.id = ?\n" +
                "\tAND t1.tenant_id = 2", join2);

        String join3 = ASTDruidUtil.addAndCondition("select t1.a, t2.b from user t1  join dept t2 on t1.dept_id = t2.id where t1.id = ?", "tenant_id = 2", "mysql");
        Assert.assertEquals("SELECT t1.a, t2.b\n" +
                "FROM user t1\n" +
                "\tJOIN dept t2\n" +
                "\tON t1.dept_id = t2.id\n" +
                "\t\tAND t2.tenant_id = 2\n" +
                "WHERE t1.id = ?\n" +
                "\tAND t1.tenant_id = 2", join3);

        // join 2个表
        String join4 = ASTDruidUtil.addAndCondition("select t1.a, t2.b from user t1 left join dept t2 on t1.dept_id = t2.id  left join dept t3 on t1.dept_id = t3.id  where t1.id = ?", "tenant_id = 2", "mysql");
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

        String join5 = ASTDruidUtil.addAndCondition("select t1.a, t2.b from user t1 right join dept t2 on t1.dept_id = t2.id right join dept t3 on t1.dept_id = t3.id where t1.id = ?", "tenant_id = 2", "mysql");
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

        String join6 = ASTDruidUtil.addAndCondition("select t1.a, t2.b from user t1 right join dept t2 on t1.dept_id = t2.id left join dept t3 on t1.dept_id = t3.id where t1.id = ?", "tenant_id = 2", "mysql");
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

        String join7 = ASTDruidUtil.addAndCondition("select t1.a, t2.b from user t1 left join dept t2 on t1.dept_id = t2.id right join dept t3 on t1.dept_id = t3.id where t1.id = ?", "tenant_id = 2", "mysql");
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

        String join8 = ASTDruidUtil.addAndCondition("select t1.a, t2.b from user t1  join dept t2 on t1.dept_id = t2.id  join dept t3 on t1.dept_id = t3.id where t1.id = ?", "tenant_id = 2", "mysql");
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

        String select1 = ASTDruidUtil.addAndCondition("select * from user  where id = ?", "tenant_id = 2", "mysql");
        Assert.assertEquals("SELECT *\n" +
                "FROM user\n" +
                "WHERE id = ?\n" +
                "\tAND user.tenant_id = 2", select1);

        String select2 = ASTDruidUtil.addAndCondition("select * from user", "tenant_id = 2", "mysql");
        Assert.assertEquals("SELECT *\n" +
                "FROM user\n" +
                "WHERE user.tenant_id = 2", select2);

        // union 联合
        String select3 = ASTDruidUtil.addAndCondition("select id,name from user t1 union select id,name from user", "tenant_id = 2", "mysql");
        Assert.assertEquals("SELECT id, name\n" +
                "FROM user t1\n" +
                "WHERE t1.tenant_id = 2\n" +
                "UNION\n" +
                "SELECT id, name\n" +
                "FROM user\n" +
                "WHERE user.tenant_id = 2", select3);

        // 子查询
        String select4 = ASTDruidUtil.addAndCondition("select * from (select id,name from user t1 union select id,name from user) t ", "tenant_id = 2", "mysql");
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

        String select5 = ASTDruidUtil.addAndCondition("select * from (select id,name from user t1 union all select id,name from user) t1 left join (select * from (select id,name from user t1 union all select id,name from user) t2) on t1.id = t2.id", "tenant_id = 2", "mysql");
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

        String select6 = ASTDruidUtil.addAndCondition(
                "select * from " +
                        "(select id,name from user t1_1 union all select id,name from user t1_2) t1 " +
                        "left join (select * from (select id,name from user t2_1_1 union all select id,name from user t2_1_2) t2_1) t2 on t1.id = t2.id " +
                        "left join user t3 on t1.id = t3.id", "tenant_id = 2", "mysql");
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

        String select61 = ASTDruidUtil.addAndCondition(
                "select * from " +
                        "(select id,name from user t1_1 union all select id,name from user t1_2) t1 " +
                        "left join (select * from (select id,name from user t2_1_1 union all select id,name from user t2_1_2) t2_1) t2 on t1.id = t2.id " +
                        "left join user t3 on t1.id = t3.id " +
                        "where id = 1 ", "tenant_id = 2", "mysql");
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

        String select7 = ASTDruidUtil.addAndCondition("select * from user left join dept on dept.id = user.dept_id", "tenant_id = 2", "mysql");
        Assert.assertEquals("SELECT *\n" +
                "FROM user\n" +
                "\tLEFT JOIN dept\n" +
                "\tON dept.id = user.dept_id\n" +
                "\t\tAND dept.tenant_id = 2\n" +
                "WHERE user.tenant_id = 2", select7);

        String select8 = ASTDruidUtil.addAndCondition("select * from user t1 left join dept on dept.id = t1.dept_id", "tenant_id = 2", "mysql");
        Assert.assertEquals("SELECT *\n" +
                "FROM user t1\n" +
                "\tLEFT JOIN dept\n" +
                "\tON dept.id = t1.dept_id\n" +
                "\t\tAND dept.tenant_id = 2\n" +
                "WHERE t1.tenant_id = 2", select8);

        String select9 = ASTDruidUtil.addAndCondition("select * from user  left join dept t1 on t1.id = user.dept_id", "tenant_id = 2", "mysql");
        Assert.assertEquals("SELECT *\n" +
                "FROM user\n" +
                "\tLEFT JOIN dept t1\n" +
                "\tON t1.id = user.dept_id\n" +
                "\t\tAND t1.tenant_id = 2\n" +
                "WHERE user.tenant_id = 2", select9);

        // or
        String select10 = ASTDruidUtil.addAndCondition("select * from user  where id = 1 or name = '1' ", "tenant_id = 2", "mysql");
        Assert.assertEquals("SELECT *\n" +
                "FROM user\n" +
                "WHERE (id = 1\n" +
                "\t\tOR name = '1')\n" +
                "\tAND user.tenant_id = 2", select10);

        String select11 = ASTDruidUtil.addAndCondition("select * from user  where (id = 1 or name = '1') and (id = 2 or name = '2') ", "tenant_id = 2", "mysql");
        Assert.assertEquals("SELECT *\n" +
                "FROM user\n" +
                "WHERE (id = 1\n" +
                "\t\tOR name = '1')\n" +
                "\tAND (id = 2\n" +
                "\t\tOR name = '2')\n" +
                "\tAND user.tenant_id = 2", select11);

        String select12 = ASTDruidUtil.addAndCondition("select * from user  where (id = 1 and name = '1') or (id = 2 and name = '2') ", "tenant_id = 2", "mysql");
        Assert.assertEquals("SELECT *\n" +
                "FROM user\n" +
                "WHERE ((id = 1\n" +
                "\t\t\tAND name = '1')\n" +
                "\t\tOR (id = 2\n" +
                "\t\t\tAND name = '2'))\n" +
                "\tAND user.tenant_id = 2", select12);

        // EXISTS
        String select13 = ASTDruidUtil.addAndCondition("select * from user  where (id = 1 and name = '1') or (id = 2 and name = '2')  and EXISTS (select id from p where p.id = id)", "tenant_id = 2", "mysql");
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

        String select14 = ASTDruidUtil.addAndCondition("select * from user  where (id = 1 and name = '1') or (id = 2 and name = '2')  and EXISTS (select id from p1 left join p2 on p2.id = p1.dept_id where p1.id = user.id)", "tenant_id = 2", "mysql");
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

        String select15 = ASTDruidUtil.addAndCondition("select case when EXISTS (select id from p1 left join p2 on p2.id = p1.dept_id where p1.id = user.id) then 1 end from user  where (id = 1 and name = '1') or (id = 2 and name = '2')  " +
                "and EXISTS (select id from p1 left join p2 on p2.id = p1.dept_id where p1.id = user.id)", "tenant_id = 2", "mysql");
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

        String select16 = ASTDruidUtil.addAndCondition("select (select count(1) from p3 where p3.id = user.id) from user  where (id = 1 and name = '1') or (id = 2 and name = '2')  " +
                "and EXISTS (select id from p1 left join p2 on p2.id = p1.dept_id where p1.id = user.id)", "tenant_id = 2", "mysql");
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

        // @
        String select17 = ASTDruidUtil.addAndCondition("SELECT\n" +
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
                "\tLIMIT 10", "tenant_id = 2", "mysql");
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

    private static void update() {
        String update1 = ASTDruidUtil.addAndCondition(" UPDATE user t1 SET `status` = 0 WHERE id = 1 or name = 2", "tenant_id = 2", "mysql");
        Assert.assertEquals("UPDATE user t1\n" +
                "SET `status` = 0\n" +
                "WHERE (id = 1\n" +
                "\t\tOR name = 2)\n" +
                "\tAND t1.tenant_id = 2", update1);

        String update2 = ASTDruidUtil.addAndCondition(" UPDATE user t1, dept t2  SET t1.`status` = t2.status WHERE t1.dept_id = t2.id", "tenant_id = 2", "mysql");
        Assert.assertEquals("UPDATE user t1, dept t2\n" +
                "SET t1.`status` = t2.status\n" +
                "WHERE t1.dept_id = t2.id\n" +
                "\tAND t1.tenant_id = 2\n" +
                "\tAND t2.tenant_id = 2", update2);

        String update3 = ASTDruidUtil.addAndCondition(" UPDATE user t1, dept t2  SET t1.`status` = t2.status WHERE t1.dept_id = t2.id and t1.id in (select id from user)", "tenant_id = 2", "mysql");
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

    private static void insert() {
        String insert1 = ASTDruidUtil.addAndCondition("insert into `base_area` (`id`, `name`) select id,name from copy ", "tenant_id = 2", "mysql");
        String insert2 = ASTDruidUtil.addAndCondition("replace into `base_area` (`id`, `name`) select id,name from copy ", "tenant_id = 2", "mysql");
        String insert3 = ASTDruidUtil.addAndCondition("select * into p_user_1 FROM p_user  ", "tenant_id = 2", "mysql");

        System.out.println("insert");
    }

    private static void delete() {
        String delete1 = ASTDruidUtil.addAndCondition("delete from user", "tenant_id = 2", "mysql");
        Assert.assertEquals("DELETE FROM user\n" +
                "WHERE user.tenant_id = 2", delete1);

        String delete2 = ASTDruidUtil.addAndCondition(" delete from user t1  WHERE id = 1 or name = 2", "tenant_id = 2", "mysql");
        Assert.assertEquals("DELETE FROM user t1\n" +
                "WHERE (id = 1\n" +
                "\t\tOR name = 2)\n" +
                "\tAND t1.tenant_id = 2", delete2);

        String delete3 = ASTDruidUtil.addAndCondition(" delete from user t1, dept t2 WHERE t1.dept_id = t2.id", "tenant_id = 2", "mysql");
        Assert.assertEquals("DELETE FROM user t1, dept t2\n" +
                "WHERE t1.dept_id = t2.id\n" +
                "\tAND t1.tenant_id = 2\n" +
                "\tAND t2.tenant_id = 2", delete3);

        String delete4 = ASTDruidUtil.addAndCondition(" delete from user t1, dept t2 WHERE t1.dept_id = t2.id and t1.id in (select id from user)", "tenant_id = 2", "mysql");
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
