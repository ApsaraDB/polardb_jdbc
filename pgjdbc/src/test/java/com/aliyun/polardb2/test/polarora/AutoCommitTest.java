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
import java.sql.Statement;
import java.util.Properties;

public class AutoCommitTest {
  private Connection conn;

  // Set up the fixture for this testcase: the tables for this test.
  @Before
  public void setUp() throws Exception {
    conn = TestUtil.openDB();
    TestUtil.createTable(conn, "test_a", "imagename name,image oid,id int4");
    TestUtil.closeDB(conn);
  }

  // Tear down the fixture for this test case.
  @After
  public void tearDown() throws Exception {
    TestUtil.closeDB(conn);
    conn = TestUtil.openDB();
    TestUtil.dropTable(conn, "test_a");
    TestUtil.closeDB(conn);
  }

  @Test
  public void testAutoCommit1() throws Exception {
    Properties props = new Properties();
    props.put("autoCommit", "true");
    conn = TestUtil.openDB(props);
    Assert.assertTrue(conn.getAutoCommit());
    conn.close();

    props = new Properties();
    props.put("autoCommit", "false");
    conn = TestUtil.openDB(props);
    Assert.assertTrue(!conn.getAutoCommit());
    conn.close();

  }

  @Test
  public void testAutoCommit2() throws Exception {
    Properties props = new Properties();
    props.put("autoCommitSpecCompliant", "false");
    conn = TestUtil.openDB(props);
    Statement st;
    ResultSet rs;

    // Now test commit
    st = conn.createStatement();
    st.executeUpdate("insert into test_a (imagename,image,id) values ('comttest',1234,5678)");
    conn.setAutoCommit(true);

    // Now update image to 9876 and commit
    st.executeUpdate("update test_a set image=9876 where id=5678");
    conn.commit();
    rs = st.executeQuery("select image from test_a where id=5678");
    Assert.assertTrue(rs.next());
    Assert.assertEquals(9876, rs.getInt(1));
    rs.close();

    // Now try to change it but rollback
    st.executeUpdate("update test_a set image=1111 where id=5678");

    // useless AUTOCOMMIT is on
    conn.rollback();
    rs = st.executeQuery("select image from test_a where id=5678");
    Assert.assertTrue(rs.next());
    Assert.assertEquals(1111, rs.getInt(1)); // Should not change!
    rs.close();

    TestUtil.closeDB(conn);
  }

}
