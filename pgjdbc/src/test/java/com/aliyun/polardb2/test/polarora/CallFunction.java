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
  }

  @After
  public void tearDown() throws Exception {
    TestUtil.execute(conn, "DROP Function Test_Jdbc_Func;");
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

}
