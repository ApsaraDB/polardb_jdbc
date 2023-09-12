/*
 * Portions Copyright (c) 2023, Alibaba Group Holding Limited
 */

package com.aliyun.polardb2.test.polarora;

import com.aliyun.polardb2.test.TestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.Connection;

public class CreateTrigger {
  private Connection conn;

  @Before
  public void setUp() throws Exception {
    conn = TestUtil.openDB();
    TestUtil.execute(conn, "CREATE TABLE trigger_test(x int, y int);");
  }

  @After
  public void tearDown() throws Exception {
    TestUtil.execute(conn, "DROP TABLE trigger_test;");
  }

  @Test
  public void testGetColumns1() throws Exception {
    CallableStatement ps = conn.prepareCall("CREATE OR REPLACE TRIGGER tr "
        + "after insert on trigger_test for each row "
        + "BEGIN perform DBMS_OUTPUT.PUT_LINE('buffer for tigger: ' || :new.x || ' '|| :new.y); "
        + "END; ");
    ps.execute();
  }

}
