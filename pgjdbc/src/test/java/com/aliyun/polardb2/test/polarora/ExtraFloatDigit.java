/*
 * Portions Copyright (c) 2023, Alibaba Group Holding Limited
 */

package com.aliyun.polardb2.test.polarora;

import com.aliyun.polardb2.test.TestUtil;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class ExtraFloatDigit {
  private Connection conn;

  @Test
  public void testDigit1() throws Exception {
    Properties props = new Properties();
    props.put("extraFloatDigits", "0");
    conn = TestUtil.openDB(props);
    Statement stmt = conn.createStatement();

    ResultSet rs = stmt.executeQuery("select 1.2345678::float;");
    Assert.assertTrue(rs.next());
    Assert.assertEquals("1.2345678", rs.getString(1));
  }

  @Test
  public void testDigit2() throws Exception {
    Properties props = new Properties();
    props.put("extraFloatDigits", "-5");
    conn = TestUtil.openDB(props);
    Statement stmt = conn.createStatement();

    ResultSet rs = stmt.executeQuery("select 0.1234567890123456789::float;");
    Assert.assertTrue(rs.next());
    Assert.assertEquals("0.123456789", rs.getString(1));
  }

  @Test
  public void testDigit3() throws Exception {
    Properties props = new Properties();
    props.put("extraFloatDigits", "-15");
    conn = TestUtil.openDB(props);
    Statement stmt = conn.createStatement();

    ResultSet rs = stmt.executeQuery("select 0.1234567890123456789::float;");
    Assert.assertTrue(rs.next());
    Assert.assertEquals("0.1", rs.getString(1));
  }

  @Test
  public void testDigit4() throws Exception {
    Properties props = new Properties();
    props.put("extraFloatDigits", "3");
    conn = TestUtil.openDB(props);
    Statement stmt = conn.createStatement();

    ResultSet rs = stmt.executeQuery("select 0.1234567890123456789::float;");
    Assert.assertTrue(rs.next());
    Assert.assertEquals("0.12345678901234568", rs.getString(1));
  }

  @Test
  public void testNstring1() throws Exception {
    Properties props = new Properties();
    conn = TestUtil.openDB(props);
    Statement stmt = conn.createStatement();

    ResultSet rs = stmt.executeQuery("select 'abcdef';");
    Assert.assertTrue(rs.next());
    Assert.assertEquals("abcdef", rs.getNString(1));
  }

}
