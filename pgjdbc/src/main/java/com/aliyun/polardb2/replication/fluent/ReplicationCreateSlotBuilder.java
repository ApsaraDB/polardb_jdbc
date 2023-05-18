/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.aliyun.polardb2.replication.fluent;

import com.aliyun.polardb2.core.BaseConnection;
import com.aliyun.polardb2.replication.fluent.logical.ChainedLogicalCreateSlotBuilder;
import com.aliyun.polardb2.replication.fluent.logical.LogicalCreateSlotBuilder;
import com.aliyun.polardb2.replication.fluent.physical.ChainedPhysicalCreateSlotBuilder;
import com.aliyun.polardb2.replication.fluent.physical.PhysicalCreateSlotBuilder;

public class ReplicationCreateSlotBuilder implements ChainedCreateReplicationSlotBuilder {
  private final BaseConnection baseConnection;

  public ReplicationCreateSlotBuilder(BaseConnection baseConnection) {
    this.baseConnection = baseConnection;
  }

  @Override
  public ChainedLogicalCreateSlotBuilder logical() {
    return new LogicalCreateSlotBuilder(baseConnection);
  }

  @Override
  public ChainedPhysicalCreateSlotBuilder physical() {
    return new PhysicalCreateSlotBuilder(baseConnection);
  }
}
