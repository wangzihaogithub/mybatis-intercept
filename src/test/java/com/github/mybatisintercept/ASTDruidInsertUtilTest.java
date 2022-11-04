package com.github.mybatisintercept;

import com.github.mybatisintercept.util.ASTDruidUtil;
import org.junit.Assert;

public class ASTDruidInsertUtilTest {

    public static void main(String[] args) {
        insert();
    }

    private static void insert() {
        String insert1 = ASTDruidUtil.addColumnValues("insert into `base_area` (`id`, `name`) select id,name from copy ", "tenant_id", 2, "mysql");
        Assert.assertEquals("INSERT INTO `base_area` (`id`, `name`, tenant_id)\n" +
                "SELECT id, name, 2\n" +
                "FROM copy", insert1);

        String insert2 = ASTDruidUtil.addColumnValues("replace into `base_area` (`id`, `name`) select id,name from copy ", "tenant_id", 2, "mysql");
        Assert.assertEquals("REPLACE INTO `base_area` (`id`, `name`, tenant_id)\n" +
                "\tSELECT id, name, 2\n" +
                "\tFROM copy", insert2);

        String insert3 = ASTDruidUtil.addColumnValues("insert into `base_area` (`id`, `name`) values (1,2), (3,4) ", "tenant_id", 2, "mysql");
        Assert.assertEquals("INSERT INTO `base_area` (`id`, `name`, tenant_id)\n" +
                "VALUES (1, 2, 2),\n" +
                "\t(3, 4, 2)", insert3);

        String insert4 = ASTDruidUtil.addColumnValues("replace into `base_area` (`id`, `name`) values (1,2), (3,4) ", "tenant_id", 2, "mysql");
        Assert.assertEquals("REPLACE INTO `base_area` (`id`, `name`, tenant_id)\n" +
                "VALUES (1, 2, 2), (3, 4, 2)", insert4);

        String insert5 = ASTDruidUtil.addColumnValues("insert ignore into `base_area` (`id`, `name`) values (1,2), (3,4) ", "tenant_id", 2, "mysql");
        Assert.assertEquals("INSERT IGNORE INTO `base_area` (`id`, `name`, tenant_id)\n" +
                "VALUES (1, 2, 2),\n" +
                "\t(3, 4, 2)", insert5);

        String insert6 = ASTDruidUtil.addColumnValues("insert ignore into `base_area` (`id`, `tenant_id`) values (1,2), (3,4) ", "tenant_id", 2, "mysql");
        Assert.assertEquals("INSERT IGNORE INTO `base_area` (`id`, `tenant_id`)\n" +
                "VALUES (1, 2),\n" +
                "\t(3, 2)", insert6);

        System.out.println();
    }
}
