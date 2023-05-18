/*
 * Copyright (c) 2021, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.aliyun.polardb2.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.aliyun.polardb2.core.ServerVersion;
import com.aliyun.polardb2.largeobject.LargeObjectManager;
import com.aliyun.polardb2.test.TestUtil;
import com.aliyun.polardb2.util.PSQLException;
import com.aliyun.polardb2.util.PSQLState;

import org.junit.Assume;
import org.junit.jupiter.api.Test;

import java.sql.Statement;

class LargeObjectManagerTest {

  /*
   * It is possible for PostgreSQL to send a ParameterStatus message after an ErrorResponse
   * Receiving such a message should not lead to an invalid connection state
   * See https://github.com/pgjdbc/pgjdbc/issues/2237
   */
  @Test
  public void testOpenWithErrorAndSubsequentParameterStatusMessageShouldLeaveConnectionInUsableStateAndUpdateParameterStatus() throws Exception {
    try (PgConnection con = (PgConnection) TestUtil.openDB()) {
      Assume.assumeTrue(TestUtil.haveMinimumServerVersion(con, ServerVersion.v9_0));
      con.setAutoCommit(false);
      String originalApplicationName = con.getParameterStatus("application_name");
      try (Statement statement = con.createStatement()) {
        statement.execute("begin;");
        // Set transaction application_name to trigger ParameterStatus message after error
        // https://www.postgresql.org/docs/14/protocol-flow.html#PROTOCOL-ASYNC
        String updatedApplicationName = "LargeObjectManagerTest-application-name";
        statement.execute("set application_name to '" + updatedApplicationName + "'");

        LargeObjectManager loManager = con.getLargeObjectAPI();
        try {
          loManager.open(0, false);
          fail("Succeeded in opening a nonexistent large object");
        } catch (PSQLException e) {
          assertEquals(PSQLState.UNDEFINED_OBJECT.getState(), e.getSQLState());
        }

        // Should be reset to original application name
        assertEquals(originalApplicationName, con.getParameterStatus("application_name"));
      }
    }
  }
}
