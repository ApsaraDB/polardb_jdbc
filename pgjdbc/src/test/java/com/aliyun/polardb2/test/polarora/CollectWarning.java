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
import java.sql.Statement;
import java.util.Properties;

public class CollectWarning {
  private Connection conn;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.put("collectWarning", "false");
    conn = TestUtil.openDB(props);
  }

  @After
  public void tearDown() throws Exception {
    conn.close();
  }

  @Test
  public void testWarning() throws Exception {
    Statement stmt = conn.createStatement();
    // Will generate a NOTICE: for primary key index creation
    stmt.execute("COMMIT");
    Assert.assertNull(stmt.getWarnings());
    stmt.close();
  }

}
