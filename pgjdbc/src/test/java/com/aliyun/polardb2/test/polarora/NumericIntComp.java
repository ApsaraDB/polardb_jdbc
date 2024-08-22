/*
 * Portions Copyright (c) 2023, Alibaba Group Holding Limited
 */

package com.aliyun.polardb2.test.polarora;

import com.aliyun.polardb2.test.TestUtil;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;

public class NumericIntComp {
  private Connection conn;

  @Before
  public void setUp() throws Exception {
    conn = TestUtil.openDB();
    TestUtil.execute(conn, "CREATE or replace PROCEDURE test_procedure(a IN number, b INOUT int, c OUT number) AS \n"
        + "DECLARE\n"
        + "userid int;\n"
        + "BEGIN\n"
        + "  \tc = a + b;\n"
        + "    b = a;\n"
        + "END;");

    conn = TestUtil.openDB();
    TestUtil.execute(conn, "CREATE or replace PROCEDURE test_procedure2(a inout char, b inout varchar) AS \n"
        + "DECLARE\n"
        + "userid int;\n"
        + "BEGIN\n"
        + "  a = 'xxx' || a;\n"
        + "  b = 'yyyy' || b;\n"
        + "END;");
  }

  @After
  public void tearDown() throws Exception {
    TestUtil.execute(conn, "drop procedure test_procedure;");
    TestUtil.execute(conn, "drop procedure test_procedure2;");
  }

  @Test
  public void testGetColumns1() throws Exception {
    CallableStatement ps = conn.prepareCall("call test_procedure(?,?,?)");
    ps.setInt(1, 1);
    ps.setInt(2, 2);
    ps.registerOutParameter(2, Types.NUMERIC);
    ps.registerOutParameter(3, Types.NUMERIC);
    ps.execute();

    Assert.assertEquals(1, ps.getInt(2));
    Assert.assertEquals(3, ps.getInt(3));
  }

  @Test
  public void testGetColumns2() throws Exception {
    CallableStatement ps = conn.prepareCall("call test_procedure(?,?,?)");
    ps.setInt(1, 1);
    ps.setInt(2, 2);
    ps.registerOutParameter(2, Types.INTEGER);
    ps.registerOutParameter(3, Types.INTEGER);
    ps.execute();

    Assert.assertEquals(1, ps.getInt(2));
    Assert.assertEquals(3, ps.getInt(3));
  }

  @Test
  public void testGetColumns3() throws Exception {
    CallableStatement ps = conn.prepareCall("call test_procedure2(?,?)");
    ps.setString(1, "aaa");
    ps.setString(2, "bbbb");
    ps.registerOutParameter(1, Types.VARCHAR);
    ps.registerOutParameter(2, Types.VARCHAR);
    ps.execute();

    Assert.assertEquals("xxxaaa", ps.getString(1));
    Assert.assertEquals("yyyybbbb", ps.getString(2));
  }

  @Test
  public void testGetColumns4() throws Exception {
    CallableStatement ps = conn.prepareCall("call test_procedure2(?,?)");
    ps.setString(1, "aaa");
    ps.setString(2, "bbbb");
    ps.registerOutParameter(1, Types.CHAR);
    ps.registerOutParameter(2, Types.CHAR);
    ps.execute();

    Assert.assertEquals("xxxaaa", ps.getString(1));
    Assert.assertEquals("yyyybbbb", ps.getString(2));
  }

}
