package com.github.mybatisintercept.util;

import org.junit.Assert;

public class ASTDruidUtilTest {
    public static void main(String[] args) {
        columnParameterizedIndex();
    }

    public static void columnParameterizedIndex() {
        int case1 = ASTDruidUtil.getColumnParameterizedIndex(
                "insert into x (a,b,c) values (1,?,?)", "b", "mysql");
        Assert.assertEquals(0, case1);

        int case2 = ASTDruidUtil.getColumnParameterizedIndex(
                "insert into x (a,b,c) values (1,?,?)", "c", "mysql");
        Assert.assertEquals(1, case2);

        int case3 = ASTDruidUtil.getColumnParameterizedIndex(
                "insert into x (a,b,c) values (1,?,?)", "a", "mysql");
        Assert.assertEquals(-2, case3);

        int case4 = ASTDruidUtil.getColumnParameterizedIndex(
                "insert into x (a,b,c) values (1,?,?)", "d", "mysql");
        Assert.assertEquals(-1, case4);
    }
}
