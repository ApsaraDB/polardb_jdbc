/*
 * Portions Copyright (c) 2023, Alibaba Group Holding Limited
 */

package com.aliyun.polardb2.test.polarora;

import com.aliyun.polardb2.test.TestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Struct;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.Properties;

public class CreateStruct {
  private Connection conn;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.put("boolAsInt", "true");
    conn = TestUtil.openDB(props);

    TestUtil.execute(conn, "create type test_object as (\n"
        + "    a1  int,\n"
        + "    a2  number,\n"
        + "    a3  boolean,\n"
        + "    a4  date,\n"
        + "    a5  timestamp,\n"
        + "    a6  varchar,\n"
        + "    a7  text,\n"
        + "    a8  text\n"
        + ");\n"
        + "\n");

    TestUtil.execute(conn, "create or replace function test_object_func (t test_object) return text as\n"
        + "begin\n"
        + "    return 'a1: ' || t.a1 || ' a2: ' || t.a2 || ' a3: ' || t.a3 || ' a4: ' || t.a4 || ' a5: ' || t.a5 || ' a6: ' || t.a6 || 'a7: ' || t.a7 || ' a8: ' || t.a8;\n"
        + "end;\n");
  }

  @After
  public void tearDown() throws Exception {
    TestUtil.execute(conn, "drop type test_object cascade;");
    conn.close();
  }

  @Test
  public void testSelectBoolean1() throws Exception {
    Object[] addressAttributes = new Object[] {
        Integer.valueOf(42),                     // Integer
        new BigDecimal("9999.99"),               // java.math.BigDecimal
        Boolean.TRUE,                            // Boolean
        new Date(),                              // java.util.Date
        new Timestamp(System.currentTimeMillis()), // java.sql.Timestamp
        "这是一个测试字符串",                      // String
        new StringBuilder("可变字符串"),           // StringBuilder
        null,                                    // null
    };
    Struct addressStruct = conn.createStruct("test_object", addressAttributes);

    CallableStatement stmt = conn.prepareCall("{? = call test_object_func(?)}");
    stmt.registerOutParameter(1, Types.VARCHAR);
    stmt.setObject(2, addressStruct);
    stmt.execute();

    System.out.println(stmt.getObject(1).toString());
  }
}
