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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class BooleanAsInt {
  private Connection conn;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.put("boolAsInt", "true");
    conn = TestUtil.openDB(props);

    TestUtil.execute(conn, "create table a(a varchar);");
  }

  @After
  public void tearDown() throws Exception {
    TestUtil.execute(conn, "drop table a;");
    conn.close();
  }

  @Test
  public void testSelectBoolean1() throws Exception {
    Statement stmt = conn.createStatement();
    ResultSet resultSet = null;
    String sql1 = "insert into a values (?)";
    String sql2 = "select * from a where a = ?";

    CallableStatement callstmt = conn.prepareCall(sql1);
    callstmt.setBoolean(1, true);
    callstmt.execute();

    callstmt.setBoolean(1, false);
    callstmt.execute();

    callstmt = conn.prepareCall(sql2);
    callstmt.setBoolean(1, true);
    resultSet = callstmt.executeQuery();

    Assert.assertTrue(resultSet.next());

    callstmt = conn.prepareCall(sql2);
    callstmt.setBoolean(1, false);
    resultSet = callstmt.executeQuery();

    Assert.assertTrue(resultSet.next());

    stmt.close();
  }
}
