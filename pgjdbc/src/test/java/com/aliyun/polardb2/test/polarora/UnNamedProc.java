/*-----------------------------------------------------------------------------
 * PolarUnamedProcedure.java -- JUnit test cases for TERSE
 *
 * Copyright (c) 2004 - 2021, PolarDB Corporation.  All rights reserved.
 *
 *
 *-----------------------------------------------------------------------------
 */

package com.aliyun.polardb2.test.polarora;

import com.aliyun.polardb2.test.TestUtil;
import com.aliyun.polardb2.test.jdbc2.BaseTest4;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;

/**
 * polar unamed procedure test.
 */
public class UnNamedProc extends BaseTest4 {
  private Connection con;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.put("unnamedProc", "true");
    con = TestUtil.openDB(props);
    TestUtil.createTable(con, "cleantableconfig", "configname varchar, tablename varchar, "
        + "executesql varchar, keepdays int, batchSize int, enabled int");
    con.setAutoCommit(false);
  }

  @After
  public void tearDown() throws SQLException {
    TestUtil.dropTable(con, "cleantableconfig");
    con.commit();
    super.tearDown();
  }

  @Test
  public void testPrepareDeclare() throws Exception {
    String sql = "declare a integer; begin a := ?; end;";
    PreparedStatement pstmt = con.prepareStatement(sql);
    pstmt.setInt(1, 100);
    pstmt.execute();
    con.commit();
    pstmt.close();
    System.out.println("Polar Unamed Procedure test 1 success");
  }

  @Test
  public void testPrepareBegin() throws Exception {
    String sql = "begin dbms_output.put_line(?); end";
    PreparedStatement pstmt = con.prepareStatement(sql);
    pstmt.setInt(1, 100);
    pstmt.execute();
    con.commit();
    pstmt.close();
    System.out.println("Polar Unamed Procedure test 2 success");
  }

  @Test
  public void testCallDeclare() throws Exception {
    String sql = "declare a integer; b integer; begin a := ?; ? := a + 1; end;";
    CallableStatement callstmt = con.prepareCall(sql);
    callstmt.setInt(1, 100);
    callstmt.registerOutParameter(2, Types.INTEGER);
    callstmt.execute();
    con.commit();
    int ret = callstmt.getInt(2);
    Assert.assertEquals(ret, 101);
    System.out.println("Polar Unamed Procedure test 3 success");
  }

  @Test
  public void testCallBegin() throws Exception {
    String sql = "begin dbms_output.put_line(?);end";
    CallableStatement callstmt = con.prepareCall(sql);
    callstmt.setInt(1, 200);
    callstmt.execute();
    con.commit();
    System.out.println("Polar Unamed Procedure test 4 success");
  }

  @Test
  public void testCallError() throws Exception {
    String sql = "begin dbms_output.put_xxxline(?);end";
    CallableStatement callstmt = con.prepareCall(sql);
    callstmt.setInt(1, 200);
    try {
      callstmt.execute();
    } catch (SQLException e) {
      /*Ignore*/
    } finally {
      Statement stmt = con.createStatement();
      con.rollback();

      ResultSet st = stmt.executeQuery("select count(*) from pg_proc where proname like "
          + "'polar_unamed_proc_%';");
      st.next();
      //Assert.assertEquals(0, st.getInt(1));
      System.out.println("Polar Unamed Procedure test 6 success");
    }
    con.commit();
    System.out.println("Polar Unamed Procedure test 5 success");
  }

  @Test
  public void testCallError2() throws Exception {
    con.setAutoCommit(true);
    String sql = "begin dbms_output.put_xxxline(?);end";
    CallableStatement callstmt = con.prepareCall(sql);
    callstmt.setInt(1, 200);

    try {
      callstmt.execute();
    } catch (SQLException e) {
      /*Ignore*/
    } finally {
      Statement stmt = con.createStatement();
      ResultSet st = stmt.executeQuery("select count(*) from pg_proc where proname like "
          + "'polar_unamed_proc_%';");
      st.next();
      //Assert.assertEquals(0, st.getInt(1));
      System.out.println("Polar Unamed Procedure test 6 success");
      con.setAutoCommit(false);
    }
  }

  @Test
  public void testComments1() throws Exception {
    String sql = "declare\n"
        + "  vCount int;\n"
        + "begin\n"
        + "  select count(1) into vCount from cleantableconfig where configname = 'VDRAGMTNOTICE' "
        + "and TABLENAME = 'VDRAGMTDTLINVNOTICE' and rownum < 2;\n"
        + "  if vCount = 0 then\n"
        + "    insert into cleantableconfig(configname, tablename, executesql, keepdays, batchSize,"
        + "\n"
        + "      enabled)\n"
        + "\t  values ('VDRAGMTNOTICE', 'VDRAGMTDTLINVNOTICE', 'delete from vdragmtdtlinvnotice "
        + "where uuid in (select o.uuid from vdragmtdtlinvnotice o where o.createTime < ?)', 30, "
        + "10000,\n"
        + "\t    0);\n"
        + "  end if;\n"
        + "  commit;\n"
        + "end;\n";

    CallableStatement callstmt = con.prepareCall(sql);
    callstmt.execute();
    con.commit();
    System.out.println("Polar Unamed Procedure test 4 success");
  }

  @Test
  public void testComments2() throws Exception {
    String sql = "declare\n"
        + "  vCount int;\n"
        + "begin\n"
        + "  select count(1) into vCount from cleantableconfig where configname = 'VDRAGMTNOTICE' "
        + "and TABLENAME = 'VDRAGMTDTLINVNOTICE' and rownum < 2;\n"
        + "  if vCount = 0 then\n"
        + "    insert into cleantableconfig(configname, tablename, executesql, keepdays, batchSize,"
        + "\n"
        + "      enabled)\n"
        + "\t  values ('VDRAGMTNOTICE', 'VDRAGMTDTLINVNOTICE', 'delete from vdragmtdtlinvnotice "
        + "where uuid in (select o.uuid from vdragmtdtlinvnotice o where o.createTime < ?)', 30, "
        + "10000,\n"
        + "\t    0);\n"
        + "  end if;\n"
        + "  dbms_output.put_line(?); "
        + "  commit;\n"
        + "end;\n";
    CallableStatement callstmt = con.prepareCall(sql);
    callstmt.setInt(1, 200);
    callstmt.execute();
    con.commit();
    System.out.println("Polar Unamed Procedure test 4 success");
  }

  @Test
  public void testComments3() throws Exception {
    String sql = "declare\n"
        + "  vCount int;\n"
        + "begin\n"
        + "  select count(1) into vCount from cleantableconfig where configname = 'VDRAGMTNOTICE' "
        + "and TABLENAME = 'VDRAGMTDTLINVNOTICE' and rownum < 2;\n"
        + "  if vCount = 0 then\n"
        + "    insert into cleantableconfig(configname, tablename, executesql, keepdays, batchSize,"
        + "\n"
        + "      enabled)\n"
        + "\t  values ('VDRAGMTNOTICE', 'VDRAGMTDTLINVNOTICE', 'delete from vdragmtdtlinvnotice "
        + "where uuid in (select o.uuid from vdragmtdtlinvnotice o where o.createTime < ?)', 30, "
        + "10000,\n"
        + "\t    0);\n"
        + "  end if;\n"
        + " -- // qwrqweqwew? qweqw qwqw  qw qw  qw???????\n "
        + " -- qwrqweqwew? qweqw -- qwqw  -- ?????qw qw  qw???????\n "
        + " /* qwrqweqwew? qweqw -- qwqw  -- ?????qw qw  qw??????? */\n"
        + " raise notice ' qwrqweqwew? qweqw -- qwqw  -- ?????qw qw  qw??????? ';\n"
        + " raise notice '\" qwrqweqwew? qwe??qw -- qwqw  -- ?????qw qw  qw??????? \"';\n"
        + "  dbms_output.put_line(?); "
        + "  commit;\n"
        + "end;\n";
    CallableStatement callstmt = con.prepareCall(sql);
    callstmt.setInt(1, 200);
    callstmt.execute();
    con.commit();
    System.out.println("Polar Unamed Procedure test 4 success");
  }

  @Test
  public void testComments4() throws Exception {
    String sql = "declare\n"
        + "  vCount int;\n"
        + "begin\n"
        + "  select count(1) into vCount from cleantableconfig where configname = 'VDRAGMTNOTICE' "
        + "and TABLENAME = 'VDRAGMTDTLINVNOTICE' and rownum < 2;\n"
        + "  if vCount = 0 then\n"
        + "    insert into cleantableconfig(configname, tablename, executesql, keepdays, batchSize,"
        + "\n"
        + "      enabled)\n"
        + "\t  values ('VDRAGMTNOTICE', 'VDRAGMTDTLINVNOTICE', 'delete from vdragmtdtlinvnotice "
        + "where uuid in (select o.uuid from vdragmtdtlinvnotice o where o.createTime < ?)', 30, "
        + "10000,\n"
        + "\t    0);\n"
        + "  end if;\n"
        + " -- // qwrqweqwew? qweqw qwqw  qw qw  qw???????\n "
        + " -- qwrqweqwew? qweqw -- qwqw  -- ?????qw qw  qw???????\n "
        + " /* qwrqweqwew? qweqw -- qwqw  -- ?????qw qw  qw??????? */\n"
        + " raise notice $$asdjkq'wj\"qwe--je//qw\\???/**/qwqwqw$$;"
        + " raise notice ' qwrqweqwew? qweqw -- qwqw  -- ?????qw qw  qw??????? ';\n"
        + " raise notice '\" qwrqweqwew? qwe??qw -- qwqw  -- ?????qw qw  qw??????? \"';\n"
        + "  dbms_output.put_line(?); "
        + "  commit;\n"
        + "end;\n";
    CallableStatement callstmt = con.prepareCall(sql);
    callstmt.setInt(1, 200);
    callstmt.execute();
    con.commit();
    System.out.println("Polar Unamed Procedure test 4 success");
  }
}
