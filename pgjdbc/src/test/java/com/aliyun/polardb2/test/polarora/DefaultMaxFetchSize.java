/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.aliyun.polardb2.test.polarora;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.aliyun.polardb2.test.TestUtil;
import com.aliyun.polardb2.test.jdbc2.BaseTest4;

import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/*
 * Tests for using non-zero setFetchSize().
 */
public class DefaultMaxFetchSize extends BaseTest4 {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    TestUtil.createTable(con, "test_fetch", "value integer");
    con.setAutoCommit(false);
  }

  @Override
  public void tearDown() throws SQLException {
    if (!con.getAutoCommit()) {
      con.rollback();
    }

    con.setAutoCommit(true);
    TestUtil.dropTable(con, "test_fetch");
    super.tearDown();
  }

  protected void createRows(int count) throws Exception {
    PreparedStatement stmt = con.prepareStatement("insert into test_fetch(value) values(?)");
    for (int i = 0; i < count; ++i) {
      stmt.setInt(1, i);
      stmt.executeUpdate();
    }
  }

  private void testPolarMaxFetchSizeBase(int maxFetchSize) throws Exception {
    int[] sizes = {0, 1, 50, 100};
    createRows(100);
    con.setAutoCommit(true);
    for (int maxRows : sizes) {
      for (int fetchSize : sizes) {
        String msg =
            "maxRows=" + maxRows + " fetchsize=" + fetchSize + " maxfetchsize=" + maxFetchSize;
        Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE);
        stmt.setMaxRows(maxRows);
        stmt.setFetchSize(fetchSize);

        ResultSet rs = stmt.executeQuery("select * from test_fetch order by value");
        assertTrue(msg, rs.isBeforeFirst());
        if (maxFetchSize != 0 && (maxRows == 0 || maxFetchSize < maxRows)) {
          int i = 0;
          try {
            while (rs.next()) {
              i++;
            }
          } catch (Exception e) {
            assertTrue(msg, e.getMessage().contains("does not exist"));
            assertEquals(msg, i, maxFetchSize);
          }
        } else {
          while (rs.next()) {
            /* PASS */
          }
          assertTrue(msg, rs.isAfterLast());
        }
      }
    }

    con.setAutoCommit(false);
    for (int maxRows : sizes) {
      for (int fetchSize : sizes) {
        String msg =
            "maxRows=" + maxRows + " fetchsize=" + fetchSize + " maxfetchsize=" + maxFetchSize;
        Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE);
        stmt.setMaxRows(maxRows);
        stmt.setFetchSize(fetchSize);
        if (fetchSize == 0 || maxFetchSize == 0) {
          maxFetchSize = Math.max(fetchSize, maxFetchSize);
        } else {
          maxFetchSize = Math.min(fetchSize, maxFetchSize);
        }
        ResultSet rs = stmt.executeQuery("select * from test_fetch order by value");
        assertTrue(msg, rs.isBeforeFirst());
        if (maxFetchSize != 0 && (maxRows == 0 || maxFetchSize < maxRows)) {
          int i = 0;
          try {
            while (rs.next()) {
              i++;
            }
          } catch (Exception e) {
            assertTrue(msg, e.getMessage().contains("does not exist"));
            assertEquals(msg, i, maxFetchSize);
          }
        } else {
          while (rs.next()) {
            /* PASS */
          }
          assertTrue(msg, rs.isAfterLast());
        }
      }
    }
    con.commit();

    con.setAutoCommit(false);
    for (int maxRows : sizes) {
      for (int fetchSize : sizes) {
        String msg =
            "maxRows=" + maxRows + " fetchsize=" + fetchSize + " maxfetchsize=" + maxFetchSize;
        Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE);
        stmt.setMaxRows(maxRows);
        stmt.setFetchSize(fetchSize);
        if (fetchSize == 0 || maxFetchSize == 0) {
          maxFetchSize = Math.max(fetchSize, maxFetchSize);
        } else {
          maxFetchSize = Math.min(fetchSize, maxFetchSize);
        }
        con.commit();
        ResultSet rs = stmt.executeQuery("select * from test_fetch order by value");
        assertTrue(msg, rs.isBeforeFirst());
        if (maxFetchSize != 0 && (maxRows == 0 || maxFetchSize < maxRows)) {
          int i = 0;
          try {
            while (rs.next()) {
              i++;
            }
          } catch (Exception e) {
            assertTrue(msg, e.getMessage().contains("does not exist"));
            assertEquals(msg, i, maxFetchSize);
          }
        } else {
          while (rs.next()) {
            /* PASS */
          }
          assertTrue(msg, rs.isAfterLast());
        }
      }
    }
    con.commit();
  }

  @Test
  public void testPolarMaxFetchSize1() throws Exception {
    int[] sizes = {0, 1, 50, 100};
    for (int maxRows : sizes) {
      con.close();
      Properties props = new Properties();
      props.put("defaultPolarMaxFetchSize", maxRows);
      con = TestUtil.openDB(props);
      testPolarMaxFetchSizeBase(maxRows);
    }
  }

}
