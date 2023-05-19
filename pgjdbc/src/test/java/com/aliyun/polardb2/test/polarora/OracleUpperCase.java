/*
 * Portions Copyright (c) 2023, Alibaba Group Holding Limited
 */

package com.aliyun.polardb2.test.polarora;

import com.aliyun.polardb2.test.TestUtil;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

public class OracleUpperCase {
  private Connection conn;

  @Before
  public void setUp() throws Exception {
    conn = TestUtil.openDB();
    TestUtil.execute(conn, "create table abc(abc int, \"dEf\" int, \"GHI\" int);");
    TestUtil.execute(conn, "create table \"dEf\"(abc int, \"dEf\" int, \"GHI\" int);");
    TestUtil.execute(conn, "create table \"GHI\"(abc int, \"dEf\" int, \"GHI\" int);");
    TestUtil.execute(conn, "insert into abc values (1, 1, 1);");
    TestUtil.execute(conn, "insert into  \"dEf\" values (1, 1, 1);");
    TestUtil.execute(conn, "insert into \"GHI\" values (1, 1, 1);");
  }

  @After
  public void tearDown() throws Exception {
    TestUtil.execute(conn, "drop table abc;");
    TestUtil.execute(conn, "drop table \"dEf\";");
    TestUtil.execute(conn, "drop table \"GHI\" ;");
  }

  @Test
  public void testTableName1() throws Exception {
    Statement stmt = conn.createStatement();

    ResultSet rs = stmt.executeQuery("select * from abc;");
    ResultSetMetaData rsmd = rs.getMetaData();
    Assert.assertEquals(rsmd.getTableName(1), "abc");

    rs = stmt.executeQuery("select * from \"dEf\";");
    rsmd = rs.getMetaData();
    Assert.assertEquals(rsmd.getTableName(1), "dEf");

    rs = stmt.executeQuery("select * from \"GHI\";");
    rsmd = rs.getMetaData();
    Assert.assertEquals(rsmd.getTableName(1), "GHI");
  }

  @Test
  public void testTableName2() throws Exception {
    Properties props = new Properties();
    props.put("oracleCase", "true");
    conn = TestUtil.openDB(props);
    Statement stmt = conn.createStatement();

    ResultSet rs = stmt.executeQuery("select * from abc;");
    ResultSetMetaData rsmd = rs.getMetaData();
    Assert.assertEquals(rsmd.getTableName(1), "ABC");

    rs = stmt.executeQuery("select * from \"dEf\";");
    rsmd = rs.getMetaData();
    Assert.assertEquals(rsmd.getTableName(1), "DEF");

    rs = stmt.executeQuery("select * from \"GHI\";");
    rsmd = rs.getMetaData();
    Assert.assertEquals(rsmd.getTableName(1), "GHI");
  }

  @Test
  public void testTableName3() throws Exception {
    Properties props = new Properties();
    props.put("oracleCase", "strict");
    conn = TestUtil.openDB(props);
    Statement stmt = conn.createStatement();

    ResultSet rs = stmt.executeQuery("select * from abc;");
    ResultSetMetaData rsmd = rs.getMetaData();
    Assert.assertEquals(rsmd.getTableName(1), "ABC");

    rs = stmt.executeQuery("select * from \"dEf\";");
    rsmd = rs.getMetaData();
    Assert.assertEquals(rsmd.getTableName(1), "dEf");

    rs = stmt.executeQuery("select * from \"GHI\";");
    rsmd = rs.getMetaData();
    Assert.assertEquals(rsmd.getTableName(1), "GHI");
  }

  @Test
  public void testTableName4() throws Exception {
    Properties props = new Properties();
    props.put("oracleCase", "false");
    conn = TestUtil.openDB(props);
    Statement stmt = conn.createStatement();

    ResultSet rs = stmt.executeQuery("select * from abc;");
    ResultSetMetaData rsmd = rs.getMetaData();
    Assert.assertEquals(rsmd.getColumnLabel(1), "abc");
    Assert.assertEquals(rsmd.getColumnLabel(2), "dEf");
    Assert.assertEquals(rsmd.getColumnLabel(3), "GHI");

    Assert.assertEquals(rsmd.getColumnName(1), "abc");
    Assert.assertEquals(rsmd.getColumnName(2), "dEf");
    Assert.assertEquals(rsmd.getColumnName(3), "GHI");
  }

  @Test
  public void testTableName5() throws Exception {
    Properties props = new Properties();
    props.put("oracleCase", "true");
    conn = TestUtil.openDB(props);
    Statement stmt = conn.createStatement();

    ResultSet rs = stmt.executeQuery("select * from abc;");
    ResultSetMetaData rsmd = rs.getMetaData();
    Assert.assertEquals(rsmd.getColumnLabel(1), "ABC");
    Assert.assertEquals(rsmd.getColumnLabel(2), "DEF");
    Assert.assertEquals(rsmd.getColumnLabel(3), "GHI");

    Assert.assertEquals(rsmd.getColumnName(1), "ABC");
    Assert.assertEquals(rsmd.getColumnName(2), "DEF");
    Assert.assertEquals(rsmd.getColumnName(3), "GHI");
  }

  @Test
  public void testTableName6() throws Exception {
    Properties props = new Properties();
    props.put("oracleCase", "strict");
    conn = TestUtil.openDB(props);
    Statement stmt = conn.createStatement();

    ResultSet rs = stmt.executeQuery("select * from abc;");
    ResultSetMetaData rsmd = rs.getMetaData();
    Assert.assertEquals(rsmd.getColumnLabel(1), "ABC");
    Assert.assertEquals(rsmd.getColumnLabel(2), "dEf");
    Assert.assertEquals(rsmd.getColumnLabel(3), "GHI");

    Assert.assertEquals(rsmd.getColumnName(1), "ABC");
    Assert.assertEquals(rsmd.getColumnName(2), "dEf");
    Assert.assertEquals(rsmd.getColumnName(3), "GHI");
  }

}
