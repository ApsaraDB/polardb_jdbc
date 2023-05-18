/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.aliyun.polardb2.test.jdbc2;

import com.aliyun.polardb2.core.AsciiStringInternerTest;
import com.aliyun.polardb2.core.CommandCompleteParserNegativeTest;
import com.aliyun.polardb2.core.CommandCompleteParserTest;
import com.aliyun.polardb2.core.OidToStringTest;
import com.aliyun.polardb2.core.OidValueOfTest;
import com.aliyun.polardb2.core.ParserTest;
import com.aliyun.polardb2.core.ReturningParserTest;
import com.aliyun.polardb2.core.UTF8EncodingTest;
import com.aliyun.polardb2.core.v3.V3ParameterListTests;
import com.aliyun.polardb2.core.v3.adaptivefetch.AdaptiveFetchCacheTest;
import com.aliyun.polardb2.jdbc.ArraysTest;
import com.aliyun.polardb2.jdbc.ArraysTestSuite;
import com.aliyun.polardb2.jdbc.BitFieldTest;
import com.aliyun.polardb2.jdbc.DeepBatchedInsertStatementTest;
import com.aliyun.polardb2.jdbc.NoColumnMetadataIssue1613Test;
import com.aliyun.polardb2.jdbc.PgSQLXMLTest;
import com.aliyun.polardb2.test.core.FixedLengthOutputStreamTest;
import com.aliyun.polardb2.test.core.JavaVersionTest;
import com.aliyun.polardb2.test.core.LogServerMessagePropertyTest;
import com.aliyun.polardb2.test.core.NativeQueryBindLengthTest;
import com.aliyun.polardb2.test.core.OptionsPropertyTest;
import com.aliyun.polardb2.test.util.ByteBufferByteStreamWriterTest;
import com.aliyun.polardb2.test.util.ByteStreamWriterTest;
import com.aliyun.polardb2.test.util.ExpressionPropertiesTest;
import com.aliyun.polardb2.test.util.HostSpecTest;
import com.aliyun.polardb2.test.util.LruCacheTest;
import com.aliyun.polardb2.test.util.PGPropertyMaxResultBufferParserTest;
import com.aliyun.polardb2.test.util.ServerVersionParseTest;
import com.aliyun.polardb2.test.util.ServerVersionTest;
import com.aliyun.polardb2.util.BigDecimalByteConverterTest;
import com.aliyun.polardb2.util.PGbyteaTest;
import com.aliyun.polardb2.util.ReaderInputStreamTest;
import com.aliyun.polardb2.util.UnusualBigDecimalByteConverterTest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/*
 * Executes all known tests for JDBC2 and includes some utility methods.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    AdaptiveFetchCacheTest.class,
    ArrayTest.class,
    ArraysTest.class,
    ArraysTestSuite.class,
    AsciiStringInternerTest.class,
    BatchedInsertReWriteEnabledTest.class,
    BatchExecuteTest.class,
    BatchFailureTest.class,
    BigDecimalByteConverterTest.class,
    BitFieldTest.class,
    BlobTest.class,
    BlobTransactionTest.class,
    ByteBufferByteStreamWriterTest.class,
    ByteStreamWriterTest.class,
    CallableStmtTest.class,
    ClientEncodingTest.class,
    ColumnSanitiserDisabledTest.class,
    ColumnSanitiserEnabledTest.class,
    CommandCompleteParserNegativeTest.class,
    CommandCompleteParserTest.class,
    ConcurrentStatementFetch.class,
    ConnectionTest.class,
    ConnectTimeoutTest.class,
    CopyLargeFileTest.class,
    CopyTest.class,
    CursorFetchTest.class,
    DatabaseEncodingTest.class,
    DatabaseMetaDataCacheTest.class,
    DatabaseMetaDataPropertiesTest.class,
    DatabaseMetaDataTest.class,
    DateStyleTest.class,
    DateTest.class,
    DeepBatchedInsertStatementTest.class,
    DriverTest.class,
    EncodingTest.class,
    ExpressionPropertiesTest.class,
    FixedLengthOutputStreamTest.class,
    GeometricTest.class,
    GetXXXTest.class,
    HostSpecTest.class,
    IntervalTest.class,
    JavaVersionTest.class,
    JBuilderTest.class,
    LoginTimeoutTest.class,
    LogServerMessagePropertyTest.class,
    LruCacheTest.class,
    MiscTest.class,
    NativeQueryBindLengthTest.class,
    NoColumnMetadataIssue1613Test.class,
    NumericTransferTest.class,
    NumericTransferTest2.class,
    NotifyTest.class,
    OidToStringTest.class,
    OidValueOfTest.class,
    OptionsPropertyTest.class,
    OuterJoinSyntaxTest.class,
    ParameterStatusTest.class,
    ParserTest.class,
    PGbyteaTest.class,
    PGPropertyMaxResultBufferParserTest.class,
    PGPropertyTest.class,
    PGTimestampTest.class,
    PGTimeTest.class,
    PgSQLXMLTest.class,
    PreparedStatementTest.class,
    QuotationTest.class,
    ReaderInputStreamTest.class,
    RefCursorTest.class,
    ReplaceProcessingTest.class,
    ResultSetMetaDataTest.class,
    ResultSetTest.class,
    ResultSetRefreshTest.class,
    ReturningParserTest.class,
    SearchPathLookupTest.class,
    ServerCursorTest.class,
    ServerErrorTest.class,
    ServerPreparedStmtTest.class,
    ServerVersionParseTest.class,
    ServerVersionTest.class,
    StatementTest.class,
    StringTypeUnspecifiedArrayTest.class,
    TestACL.class,
    TimestampTest.class,
    TimeTest.class,
    TimezoneCachingTest.class,
    TimezoneTest.class,
    TypeCacheDLLStressTest.class,
    UnusualBigDecimalByteConverterTest.class,
    UpdateableResultTest.class,
    UpsertTest.class,
    UTF8EncodingTest.class,
    V3ParameterListTests.class
})
public class Jdbc2TestSuite {
}
