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

public class InvalidEnd {
  private Connection conn;

  @Before
  public void setUp() throws Exception {
    conn = TestUtil.openDB();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testPkgRes() throws Exception {
    CallableStatement ps = conn.prepareCall("CREATE or replace PACKAGE rae_test_pkg AS\n"
        + "        FUNCTION createstkinbypickex(pinum character varying) RETURN numeric;\n"
        + "        end;");

    ps.execute();

    ps = conn.prepareCall("CREATE or replace PACKAGE body rae_test_pkg AS\n"
        + "        FUNCTION createstkinbypickex(pinum character varying) RETURN numeric IS\n"
        + "        Begin\n"
        + "            If 1 then Goto One_Goods_End;  End If;\n"
        + "            perform 1 / 1;\n"
        + "        end;\n"
        + "        END;");

    ps.execute();
  }

  @Test
  public void testPkgRes2() throws Exception {
    CallableStatement ps = conn.prepareCall("CREATE or replace PACKAGE rae_test_pkg2 AS\n"
        + "        FUNCTION createstkinbypickex(pinum character varying) RETURN numeric;\n"
        + "        end;");

    ps.execute();

    ps = conn.prepareCall("CREATE or replace PACKAGE body rae_test_pkg2 AS\n"
        + "        FUNCTION createstkinbypickex(pinum character varying) RETURN numeric IS\n"
        + "        CURSOR c_delete_table IS select 1 / 2 from dual;"
        + "        Begin\n"
        + "            If 1 then Goto One_Goods_End;  End If;\n"
        + "            perform 1 / 1;\n"
        + "        end;\n"
        + "        END;");

    ps.execute();
  }
}
