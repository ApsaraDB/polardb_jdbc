/*
 * Copyright (c) 2021, Alibaba Group Holding Limited
 */

package com.aliyun.polardb2.test.polarora;

import com.aliyun.polardb2.test.TestUtil;
import com.aliyun.polardb2.test.jdbc2.BaseTest4;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class NamedParam extends BaseTest4 {

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.put("namedParam", "true");
    con = TestUtil.openDB(props);
    TestUtil.createTable(con, "testnamedparams", "id int, name varchar(20)");

    Statement stmt = con.createStatement();
    stmt.executeUpdate("INSERT INTO testnamedparams values(1, 'test')");
  }

  @After
  public void tearDown() throws SQLException {
    TestUtil.dropTable(con, "testnamedparams");
    super.tearDown();
  }

  @Test
  public void testPolarRowId() throws Exception {
    // Test one named parameters
    String sql = "select id from testnamedparams where name = :test";
    PreparedStatement ps = con.prepareStatement(sql);
    ps.setString(1, "test");
    ResultSet rs = ps.executeQuery();
    Assert.assertTrue(rs.next());
    Assert.assertEquals(rs.getInt(1), 1);

    // Test two named parameters
    sql = "select id from testnamedparams where name = :test and id = :id ";
    ps = con.prepareStatement(sql);
    ps.setString(1, "test");
    ps.setInt(2, 1);
    rs = ps.executeQuery();
    Assert.assertTrue(rs.next());
    Assert.assertEquals(rs.getInt(1), 1);

    // Test string like named parameters
    sql = "select id from testnamedparams where name = ':test' and id = :id ";
    ps = con.prepareStatement(sql);
    ps.setInt(1, 1);
    rs = ps.executeQuery();
    Assert.assertFalse(rs.next());

    // Test named parameters and normal parameter
    sql = "select id from testnamedparams where name = :test and id = ?::int ";
    ps = con.prepareStatement(sql);
    ps.setString(1, "test");
    ps.setInt(2, 1);
    rs = ps.executeQuery();
    Assert.assertTrue(rs.next());
    Assert.assertEquals(rs.getInt(1), 1);

    // Test named parameters and cast
    sql = "select id from testnamedparams where name = :test::varchar and id = '1'::int ";
    ps = con.prepareStatement(sql);
    ps.setString(1, "test");
    rs = ps.executeQuery();
    Assert.assertTrue(rs.next());
    Assert.assertEquals(rs.getInt(1), 1);
  }
}
