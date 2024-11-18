/*
 * Copyright (c) 2024, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.aliyun.polardb2.core.v3;

import com.aliyun.polardb2.PGProperty;
import com.aliyun.polardb2.util.GT;
import com.aliyun.polardb2.util.PSQLException;
import com.aliyun.polardb2.util.PSQLState;
import com.aliyun.polardb2.util.internal.Nullness;

import java.util.Properties;

enum ChannelBindingOption {
  /**
   * Prevents the use of channel binding
   */
  DISABLE,
  /**
   * Means that the client will choose channel binding if available.
   */
  PREFER,
  /**
   * Means that the connection must employ channel binding.
   */
  REQUIRE;
  public static ChannelBindingOption of(Properties info) throws PSQLException {
    String option = Nullness.castNonNull(PGProperty.CHANNEL_BINDING.getOrDefault(info));
    switch (option) {
      case "disable":
        return DISABLE;
      case "prefer":
        return PREFER;
      case "require":
        return REQUIRE;
      default:
        throw new PSQLException(GT.tr("Invalid channelBinding value: {0}", option),
            PSQLState.CONNECTION_UNABLE_TO_CONNECT);
    }
  }
}
