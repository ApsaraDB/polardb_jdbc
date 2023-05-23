/*
 * Portions Copyright (c) 2023, Alibaba Group Holding Limited
 */

package com.aliyun.polardb2.test.polarora;

import com.aliyun.polardb2.jdbc.PgBlobBytea;
import com.aliyun.polardb2.test.TestUtil;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

public class PgBlobByteaTest {
  private Connection conn;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.put("blobAsBytea", "true");
    conn = TestUtil.openDB(props);
    TestUtil.createTable(conn, "blob_test", "id int,name blob");
  }

  @After
  public void tearDown() throws Exception {
    TestUtil.dropTable(conn, "blob_test");
  }

  @Test
  public void testBlobBytea1() throws Exception {
    String str = "abcd1234";
    byte[] bytes = str.getBytes();
    Blob blobin = new PgBlobBytea(bytes);

    Assert.assertEquals("abcd1234", new String(bytes));

    PreparedStatement pstmt1 = conn.prepareStatement("INSERT INTO blob_test(id, name) VALUES (?, "
        + "?)");
    pstmt1.setInt(1, 1);
    pstmt1.setObject(2, blobin);
    pstmt1.execute();

    PreparedStatement pstmt2 = conn.prepareStatement("select * from blob_test");
    ResultSet resultSet = pstmt2.executeQuery();
    Assert.assertTrue(resultSet.next());
    Blob blob = (Blob) resultSet.getObject(2);
    bytes = resultSet.getBytes(2);

    // test length()
    Assert.assertEquals(blob.length(), bytes.length);

    // test getBytes()
    Assert.assertEquals(str, new String(blob.getBytes(1, (int) blob.length())));
    Assert.assertEquals("1234", new String(blob.getBytes(5, (int) blob.length() - 4)));
    Assert.assertEquals("12", new String(blob.getBytes(5, (int) blob.length() - 6)));

    // test position()
    Assert.assertEquals(1, blob.position("abcd12".getBytes(), 1));
  }

}
