/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.aliyun.polardb2.replication.fluent.logical;

import com.aliyun.polardb2.replication.PGReplicationStream;

import java.sql.SQLException;

public interface StartLogicalReplicationCallback {
  PGReplicationStream start(LogicalReplicationOptions options) throws SQLException;
}
