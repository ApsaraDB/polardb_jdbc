/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.aliyun.polardb2.jdbc;

import com.aliyun.polardb2.core.Field;
import com.aliyun.polardb2.core.ParameterList;
import com.aliyun.polardb2.core.Query;
import com.aliyun.polardb2.core.ResultCursor;
import com.aliyun.polardb2.core.Tuple;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

class CallableBatchResultHandler extends BatchResultHandler {
  CallableBatchResultHandler(PgStatement statement, Query[] queries,
      @Nullable ParameterList[] parameterLists) {
    super(statement, queries, parameterLists, false);
  }

  public void handleResultRows(Query fromQuery, Field[] fields, List<Tuple> tuples,
      @Nullable ResultCursor cursor) {
    /* ignore */
  }
}
