/*
 * Copyright (c) 2023, Alibaba Group Holding Limited
 */

package com.aliyun.polardb2.test.polarora;

import com.aliyun.polardb2.test.TestUtil;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class RowidTest {
  private Connection conn;

  @Before
  public void setUp() throws Exception {
    conn = TestUtil.openDB();
    //TestUtil.execute(conn, "set polar_default_with_rowid to true");
    TestUtil.createTable(conn, "test_rowid", "id int4");
    TestUtil.execute(conn, "INSERT INTO test_rowid values(100)");
  }

  @After
  public void tearDown() throws SQLException {
    TestUtil.dropTable(conn, "test_rowid");
    TestUtil.closeDB(conn);
  }

  @Test
  public void testRowId() throws Exception {
    Statement stmt = conn.createStatement();

    ResultSet rs = stmt.executeQuery("show polar_default_with_rowid");
    Assert.assertTrue(rs.next());
    Assert.assertEquals("on", rs.getString(1));
    System.out.println("Polar Rowid test 0 success");

    rs = stmt.executeQuery("select * from test_rowid");
    // ROWID not included in A_star expr
    Assert.assertTrue(rs.next());
    Assert.assertEquals(1, rs.getMetaData().getColumnCount());
    Assert.assertEquals(100, rs.getInt(1));
    System.out.println("Polar Rowid test 1 success");

    rs = stmt.executeQuery("select rowid, id from test_rowid");
    Assert.assertTrue(rs.next());
    Assert.assertEquals(2, rs.getMetaData().getColumnCount());
    Assert.assertTrue(rs.getInt(1) > 0);
    System.out.println("Polar Rowid test 2 success");
  }
}
