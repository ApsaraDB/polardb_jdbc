/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.aliyun.polardb2.replication.fluent;

import com.aliyun.polardb2.core.BaseConnection;
import com.aliyun.polardb2.core.ReplicationProtocol;
import com.aliyun.polardb2.replication.PGReplicationStream;
import com.aliyun.polardb2.replication.fluent.logical.ChainedLogicalStreamBuilder;
import com.aliyun.polardb2.replication.fluent.logical.LogicalReplicationOptions;
import com.aliyun.polardb2.replication.fluent.logical.LogicalStreamBuilder;
import com.aliyun.polardb2.replication.fluent.logical.StartLogicalReplicationCallback;
import com.aliyun.polardb2.replication.fluent.physical.ChainedPhysicalStreamBuilder;
import com.aliyun.polardb2.replication.fluent.physical.PhysicalReplicationOptions;
import com.aliyun.polardb2.replication.fluent.physical.PhysicalStreamBuilder;
import com.aliyun.polardb2.replication.fluent.physical.StartPhysicalReplicationCallback;

import java.sql.SQLException;

public class ReplicationStreamBuilder implements ChainedStreamBuilder {
  private final BaseConnection baseConnection;

  /**
   * @param connection not null connection with that will be associate replication
   */
  public ReplicationStreamBuilder(final BaseConnection connection) {
    this.baseConnection = connection;
  }

  @Override
  public ChainedLogicalStreamBuilder logical() {
    return new LogicalStreamBuilder(new StartLogicalReplicationCallback() {
      @Override
      public PGReplicationStream start(LogicalReplicationOptions options) throws SQLException {
        ReplicationProtocol protocol = baseConnection.getReplicationProtocol();
        return protocol.startLogical(options);
      }
    });
  }

  @Override
  public ChainedPhysicalStreamBuilder physical() {
    return new PhysicalStreamBuilder(new StartPhysicalReplicationCallback() {
      @Override
      public PGReplicationStream start(PhysicalReplicationOptions options) throws SQLException {
        ReplicationProtocol protocol = baseConnection.getReplicationProtocol();
        return protocol.startPhysical(options);
      }
    });
  }
}
