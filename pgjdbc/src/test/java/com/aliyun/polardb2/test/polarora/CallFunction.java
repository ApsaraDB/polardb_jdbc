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
import java.sql.Types;
import java.util.Properties;

public class CallFunction {
  private Connection conn;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.put("callFunctionMode", "true");
    conn = TestUtil.openDB(props);
    TestUtil.execute(conn, "Create Or Replace Function Test_Jdbc_Func (\n"
        + "    piStoreGid  In      INT,      \n"
        + "    poErr_Msg   Out     Varchar2  \n"
        + "  ) Return Number\n"
        + "  Is \n"
        + "  Begin \n"
        + "    poErr_Msg := '1';\n"
        + "    return 1;\n"
        + "  End;");

    TestUtil.execute(conn, "create Or Replace procedure abc1(a int, b int) as\n"
        + "begin\n"
        + "\traise notice '11';\n"
        + "end;");

    TestUtil.execute(conn, "create Or Replace function abc2(a int, b int) return int as\n"
        + "begin\n"
        + "return a + b;\n"
        + "end;");

    TestUtil.execute(conn, "create Or Replace procedure abc3(a int, b out int) as\n"
        + "begin\n"
        + "b=3;\n"
        + "raise notice '1';\n"
        + "end;");

    TestUtil.execute(conn, "CREATE OR REPLACE Procedure Test_Proc (\n"
        + "  piBillTo         In          Int,\n"
        + "  piOcrDate        In          Date,\n"
        + "  piFilDate        In          Date,\n"
        + "  piChkDate        In          Date,\n"
        + "  piIvcEndDate     In          Date\n"
        + ") Is\n"
        + "Begin\n"
        + "  Null;\n"
        + "End;\n");
    TestUtil.execute(conn, "CREATE OR REPLACE PROCEDURE test_in_out_procedure (a IN number, b IN OUT number, c OUT number) IS\n"
        + "BEGIN\n"
        + "\tb := a + b;\n"
        + "\tc := b + 1;\n"
        + "END;");
    TestUtil.execute(conn, "CREATE OR REPLACE FUNCTION test_in_out_function (a IN number, b IN OUT number, c OUT number) RETURN number AS\n"
        + "BEGIN\n"
        + "\tb := a + b;\n"
        + "\tc := b + 1;\n"
        + "\n"
        + "\tRETURN c + 1;\n"
        + "END;");
    TestUtil.execute(conn, "CREATE OR REPLACE FUNCTION test_in_out_function2 (a IN number, b IN OUT number, c OUT number) RETURN number AS\n"
        + "BEGIN\n"
        + "\tb := a + b;\n"
        + "\tc := b + 1;\n"
        + "\n"
        + "\tRETURN null;\n"
        + "END;");
    TestUtil.execute(conn, "CREATE OR REPLACE PROCEDURE test_in_out_function_as_procedure_1 (a IN number, b IN OUT number, c OUT number, r OUT number) AS\n"
        + "BEGIN\n"
        + "\tr := test_in_out_function(a, b, c);\n"
        + "END;");
    TestUtil.execute(conn, "CREATE OR REPLACE PROCEDURE test_in_out_function_as_procedure_2 (a IN number, b IN OUT number, c OUT number, r OUT number) AS\n"
        + "BEGIN\n"
        + "\tSELECT test_in_out_function(a, b, c) INTO r from dual;\n"
        + "END;\n");
  }

  @After
  public void tearDown() throws Exception {
    TestUtil.execute(conn, "DROP Function Test_Jdbc_Func;");
    TestUtil.execute(conn, "DROP procedure abc1;");
    TestUtil.execute(conn, "DROP Function abc2;");
    TestUtil.execute(conn, "DROP procedure abc3;");
    TestUtil.execute(conn, "DROP procedure Test_Proc;");
  }

  @Test
  public void testGetColumns1() throws Exception {

    String callString = "{ ? = call Test_Jdbc_Func(?, ?) }";
    try (CallableStatement cstmt = conn.prepareCall(callString)) {
      cstmt.registerOutParameter(1, Types.NUMERIC);
      // 设置输入参数
      cstmt.setInt(2, Types.INTEGER);

      // 注册输出参数
      cstmt.registerOutParameter(3, Types.VARCHAR);

      // 执行存储过程
      cstmt.executeUpdate();

      // 输出结果
      System.out.println("v_ret: " + cstmt.getInt(1));
      System.out.println("v_ret3: " + cstmt.getString(3));
    }
  }

  @Test
  public void testGetColumns2() throws Exception {

    String callString = "{ ? = call abc2(?, ?) }";
    try (CallableStatement cstmt = conn.prepareCall(callString)) {
      cstmt.registerOutParameter(1, Types.NUMERIC);
      // 设置输入参数
      cstmt.setObject(2, 2);

      // 注册输出参数
      cstmt.setObject(3, 1);

      // 执行存储过程
      cstmt.executeUpdate();

      // 输出结果
      System.out.println("v_ret: " + cstmt.getInt(1));
    }
  }

  @Test
  public void testGetColumns3() throws Exception {

    String callString = "{call abc1(?, ?) }";
    try (CallableStatement cstmt = conn.prepareCall(callString)) {
      // 设置输入参数
      cstmt.setObject(1, 1);
      // 设置输入参数
      cstmt.setObject(2, 1);

      // 执行存储过程
      cstmt.executeUpdate();
    }
  }

  @Test
  public void testGetColumns4() throws Exception {

    String callString = "{ ? = call abc2(?, ?) }";
    try (CallableStatement cstmt = conn.prepareCall(callString)) {
      cstmt.registerOutParameter(1, Types.NUMERIC);
      // 设置输入参数
      cstmt.setInt(2, 2);

      // 注册输出参数
      cstmt.setInt(3, 1);

      // 执行存储过程
      cstmt.executeUpdate();

      // 输出结果
      System.out.println("v_ret: " + cstmt.getInt(1));
    }
  }

  @Test
  public void testGetColumns5() throws Exception {

    String callString = "{call abc3(?, ?) }";
    try (CallableStatement cstmt = conn.prepareCall(callString)) {
      // 设置输入参数
      cstmt.setObject(1, 1);
      // 设置输入参数
      cstmt.registerOutParameter(2, Types.NUMERIC);

      // 执行存储过程
      cstmt.executeUpdate();

      System.out.println("v_ret: " + cstmt.getInt(2));
    }
  }

  @Test
  public void testGetColumns6() throws Exception {
    String sql = "{call Test_Proc(?, ?, ?, ?, ?) }";
    CallableStatement pstmt = conn.prepareCall(sql);
    pstmt.setObject(1, 7000031);
    pstmt.setObject(2, null);
    pstmt.setObject(3, null);
    pstmt.setObject(4, null);
    pstmt.setObject(5, null);
    // 打印字符串长度，检查是否合理
    System.out.println("SQL length: " + sql.length());

    // 执行 SQL
    pstmt.executeUpdate();
  }

  @Test
  public void test_in_out_procedure() throws Exception {
    CallableStatement callableStatement = null;
    int a = 1;
    int b = 2;
    int c = 0;
    System.out.println(String.format("BEFORE test_in_out_procedure: a=%d, b=%d, c=%d", a, b, c));

    callableStatement = conn.prepareCall("{call test_in_out_procedure (?, ?, ?)}");
    // a
    callableStatement.setInt(1, a);
    // b
    callableStatement.setInt(2, b);
    callableStatement.registerOutParameter(2, java.sql.Types.INTEGER);
    // c
    callableStatement.registerOutParameter(3, java.sql.Types.INTEGER);
    callableStatement.execute();
    b = callableStatement.getInt(2);
    c = callableStatement.getInt(3);

    System.out.println(String.format("AFTER test_in_out_procedure: a=%d, b=%d, c=%d", a, b, c));
    System.out.println();
  }

  @Test
  public void test_in_out_procedure2() throws Exception {
    CallableStatement callableStatement = null;
    int a = 1;
    int b = 2;
    int c = 0;
    System.out.println(String.format("BEFORE test_in_out_procedure: a=%d, b=%d, c=%d", a, b, c));

    callableStatement = conn.prepareCall("BEGIN test_in_out_procedure (?, ?, ?); END;");
    // a
    callableStatement.setInt(1, a);
    // b
    callableStatement.setInt(2, b);
    callableStatement.registerOutParameter(2, java.sql.Types.INTEGER);
    // c
    callableStatement.registerOutParameter(3, java.sql.Types.INTEGER);
    callableStatement.execute();
    b = callableStatement.getInt(2);
    c = callableStatement.getInt(3);

    System.out.println(String.format("AFTER test_in_out_procedure: a=%d, b=%d, c=%d", a, b, c));
    System.out.println();
  }

  @Test
  public void test_in_out_function() throws Exception {
    CallableStatement callableStatement = null;

    int r = 0;
    int a = 1;
    int b = 2;
    int c = 0;

    System.out.println(String.format("BEFORE test_in_out_function: r=%d, a=%d, b=%d, c=%d", r, a, b, c));

    callableStatement = conn.prepareCall("{ ?= call test_in_out_function (?, ?, ?)}");

    // r
    callableStatement.registerOutParameter(1, java.sql.Types.INTEGER);

    // a
    callableStatement.setInt(2, a);

    // b
    callableStatement.setInt(3, b);
    callableStatement.registerOutParameter(3, java.sql.Types.INTEGER);

    // c
    callableStatement.registerOutParameter(4, java.sql.Types.INTEGER);

    callableStatement.execute();

    r = callableStatement.getInt(1);
    b = callableStatement.getInt(3);
    c = callableStatement.getInt(4);

    System.out.println(String.format("AFTER test_in_out_function: r=%d, a=%d, b=%d, c=%d", r, a, b, c));
    System.out.println();
  }

  @Test
  public void test_in_out_function2() throws Exception {
    CallableStatement callableStatement = null;

    int r = 0;
    int a = 1;
    int b = 2;
    int c = 0;

    System.out.println(String.format("BEFORE test_in_out_function2: r=%d, a=%d, b=%d, c=%d", r, a, b, c));

    callableStatement = conn.prepareCall("{ ?= call test_in_out_function2 (?, ?, ?)}");

    // r
    callableStatement.registerOutParameter(1, java.sql.Types.INTEGER);

    // a
    callableStatement.setInt(2, a);

    // b
    callableStatement.setInt(3, b);
    callableStatement.registerOutParameter(3, java.sql.Types.INTEGER);

    // c
    callableStatement.registerOutParameter(4, java.sql.Types.INTEGER);

    callableStatement.execute();

    r = callableStatement.getInt(1);
    b = callableStatement.getInt(3);
    c = callableStatement.getInt(4);

    System.out.println(String.format("AFTER test_in_out_function: r=%d, a=%d, b=%d, c=%d", r, a, b, c));
    System.out.println();
  }

  @Test
  public void test_in_out_function_as_anonymous_block() throws Exception {
    CallableStatement callableStatement = null;

    int r = 0;
    int a = 1;
    int b = 2;
    int c = 0;

    System.out.println(String.format("BEFORE test_in_out_function: r=%d, a=%d, b=%d, c=%d", r, a, b, c));

    callableStatement = conn.prepareCall("BEGIN ? := test_in_out_function (?, ?, ?); END;");

    // r
    callableStatement.registerOutParameter(1, java.sql.Types.INTEGER);

    // a
    callableStatement.setInt(2, a);

    // b
    callableStatement.setInt(3, b);
    callableStatement.registerOutParameter(3, java.sql.Types.INTEGER);

    // c
    callableStatement.registerOutParameter(4, java.sql.Types.INTEGER);

    callableStatement.execute();

    r = callableStatement.getInt(1);
    b = callableStatement.getInt(3);
    c = callableStatement.getInt(4);

    System.out.println(String.format("AFTER test_in_out_function: r=%d, a=%d, b=%d, c=%d", r, a, b, c));
    System.out.println();
  }

  @Test
  public void test_in_out_function_as_procedure_1() throws Exception {
    CallableStatement callableStatement = null;

    int r = 0;
    int a = 1;
    int b = 2;
    int c = 0;

    System.out.println(String.format("BEFORE proc_test_in_out_function: r=%d, a=%d, b=%d, c=%d", r, a, b, c));

    callableStatement = conn.prepareCall("{call test_in_out_function_as_procedure_1 (?, ?, ?, ?)}");

    // a
    callableStatement.setInt(1, a);

    // b
    callableStatement.setInt(2, b);
    callableStatement.registerOutParameter(2, java.sql.Types.INTEGER);

    // c
    callableStatement.registerOutParameter(3, java.sql.Types.INTEGER);

    // r
    callableStatement.registerOutParameter(4, java.sql.Types.INTEGER);

    callableStatement.execute();

    b = callableStatement.getInt(2);
    c = callableStatement.getInt(3);

    r = callableStatement.getInt(4);

    System.out.println(String.format("AFTER proc_test_in_out_function: r=%d, a=%d, b=%d, c=%d", r, a, b, c));
    System.out.println();
  }

  @Test
  public void test_in_out_function_as_procedure_2() throws Exception {
    CallableStatement callableStatement = null;

    int r = 0;
    int a = 1;
    int b = 2;
    int c = 0;

    System.out.println(String.format("BEFORE proc_test_in_out_function: r=%d, a=%d, b=%d, c=%d", r, a, b, c));

    callableStatement = conn.prepareCall("{call test_in_out_function_as_procedure_2 (?, ?, ?, ?)}");

    // a
    callableStatement.setInt(1, a);

    // b
    callableStatement.setInt(2, b);
    callableStatement.registerOutParameter(2, java.sql.Types.INTEGER);

    // c
    callableStatement.registerOutParameter(3, java.sql.Types.INTEGER);

    // r
    callableStatement.registerOutParameter(4, java.sql.Types.INTEGER);

    callableStatement.execute();

    b = callableStatement.getInt(2);
    c = callableStatement.getInt(3);

    r = callableStatement.getInt(4);

    System.out.println(String.format("AFTER proc_test_in_out_function: r=%d, a=%d, b=%d, c=%d", r, a, b, c));
    System.out.println();
  }
}
