/*
 * Copyright (c) 2021, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.aliyun.polardb2.test.polarora;

import com.aliyun.polardb2.PGProperty;
import com.aliyun.polardb2.core.QueryExecutor;
import com.aliyun.polardb2.jdbc.PgBlobBytea;
import com.aliyun.polardb2.jdbc.PgClobText;
import com.aliyun.polardb2.jdbc.PgConnection;
import com.aliyun.polardb2.test.TestUtil;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;

public class LobVarlenaTest {

  private Connection conn;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(PGProperty.BLOB_AS_BYTEA.getName(), "true");
    props.setProperty(PGProperty.CLOB_AS_TEXT.getName(), "true");
    conn = TestUtil.openDB(props);
    TestUtil.createTable(conn, "testlobvarlena",
        "textcol text, byteacol bytea");
    TestUtil.execute("insert into testlobvarlena values("
            + "repeat($$ksjkdjkdshtrb\\000ujy2t8mnnksdf$$,500),"
            + "decode(repeat($$ksjkds\\000trbut8mnnksdf$$,600),"
            + "'escape'))",
        conn);
  }

  @After
  public void tearDown() throws Exception {
    TestUtil.dropTable(conn, "testlobvarlena");
    TestUtil.closeDB(conn);
  }

  @Test
  public void testConnClob() throws SQLException {
    // test we can get a clob from the connection and do basic set, get,
    // and search operations on it
    Assert.assertTrue("Connection has clobAsText enabled", ((PgConnection) conn).getClobAsText());
    Clob clob = conn.createClob();
    Assert.assertTrue("Successfully create a Clob", clob instanceof PgClobText);
    Assert.assertEquals("Length of Clob is 0", clob.length(), 0);
    clob.setString(1, "foobarbaz");
    Assert.assertEquals("Clob length is 9", clob.length(), 9);
    Assert.assertEquals("Clob text is 'foobarbaz'", "foobarbaz", clob.toString());
    String s = clob.getSubString(4, 3);
    Assert.assertEquals("Fetched substring is 'bar'", "bar", s);
    String s2 = clob.getSubString(7, 99);
    Assert.assertEquals("Fetched substring is 'baz'", "baz", s2);
    long pos = clob.position("bar", 1);
    Assert.assertEquals("Found 'bar' at position 4 starting at 1", pos, 4);
    pos = clob.position("bar", 4);
    Assert.assertEquals("Found 'bar' at position 4 starting at 4", pos, 4);
    pos = clob.position("bar", 7);
    Assert.assertEquals("'bar' not found starting at 7", pos, -1);
    pos = clob.position("blurfl", 4);
    Assert.assertEquals("'blurfl' not found starting at 4", pos, -1);
    Clob srch = new PgClobText("bar");
    pos = clob.position(srch, 1);
    Assert.assertEquals("Found clob 'bar' at position 4 starting at 1", pos, 4);
    pos = clob.position(srch, 4);
    Assert.assertEquals("Found clob 'bar' at position 4 starting at 4", pos, 4);
    pos = clob.position(srch, 7);
    Assert.assertEquals("clob 'bar' not found starting at 7", pos, -1);
    srch.setString(1, "blurfl");
    pos = clob.position(srch, 4);
    Assert.assertEquals("clob 'blurfl' not found starting at 4", pos, -1);
    clob.truncate(3);
    Assert.assertEquals("Truncated clob length is 3", clob.length(), 3);
    String s3 = clob.toString();
    Assert.assertEquals("Truncated clob value is 'foo'", "foo", s3);
  }

  @Test
  public void testResultSetClob() throws SQLException {
    // test we can get a clob from a text field in a resultset
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(TestUtil.selectSQL("testlobvarlena", "textcol"));
    try {
      Assert.assertTrue("Connection has getClobAsText", ((PgConnection) conn).getClobAsText());
      Assert.assertTrue("Result set has a row", rs.next());
      Clob clob = rs.getClob(1);
      Assert.assertTrue("rs.getClob(1) returns a PgClobText", clob instanceof PgClobText);
    } finally {
      rs.close();
    }
  }

  @Test
  public void testClobStatementParams() throws SQLException {
    // test we can set a string or a Clob as a parameter
    conn.setAutoCommit(false);
    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO testlobvarlena (textcol) VALUES "
        + "(?)");
    pstmt.setObject(1, "foo", Types.CLOB);
    pstmt.executeUpdate();
    Clob clob = conn.createClob();
    clob.setString(1, "foobar");
    pstmt.setObject(1, clob, Types.CLOB);
    pstmt.executeUpdate();
    conn.rollback();
  }

  @Test
  public void testClobReaderStreams() throws SQLException, IOException {
    // test streams operations of a Clob
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(TestUtil.selectSQL("testlobvarlena", "textcol"));
    try {
      rs.next();
      Clob clob = rs.getClob(1);
      Reader r = clob.getCharacterStream();
      StringWriter w = new StringWriter((int) clob.length());
      int i;
      while ((i = r.read()) != -1) {
        w.write(i);
      }
      w.close();
      String cs = clob.toString();
      Assert.assertEquals("Clob contents = character stream", cs, w.toString());
      r = clob.getCharacterStream(100, 20);
      w = new StringWriter(300);
      while ((i = r.read()) != -1) {
        w.write(i);
      }
      w.close();
      String cs2 = clob.getSubString(100, 20);
      String ws = w.toString();
      Assert.assertEquals("Clob substring = character stream substring", cs2, ws);
      char[] buffer = new char[(int) clob.length()];
      InputStream is = clob.getAsciiStream();
      int index = 0;
      int c = is.read();
      while (c > 0) {
        buffer[index++] = (char) c;
        c = is.read();
      }
      String as = new String(buffer);
      Assert.assertEquals("Ascii stream equals clob contents", as, cs);
    } finally {
      rs.close();
    }
  }

  @Test
  public void testClobWriterStreams() throws SQLException, IOException {
    // test streams operations of a Clob
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(TestUtil.selectSQL("testlobvarlena", "textcol"));
    try {
      rs.next();
      Clob clob = rs.getClob(1);
      int origLen = (int) clob.length();
      OutputStream os = clob.setAsciiStream(origLen - 4);
      byte[] blurfl = "blurfl".getBytes();
      os.write(blurfl, 0, 5);
      Assert.assertEquals("Length stays the same", clob.length(), origLen);
      os.write("bazBarfoo".getBytes());
      Assert.assertEquals("Length grown by 9", clob.length(), origLen + 9);
      String end = clob.getSubString(origLen - 4, 999);
      Assert.assertEquals("Stream has written bytes correctly", "blurfbazBarfoo", end);
      clob.truncate(origLen);
      Writer w = clob.setCharacterStream(origLen - 4);
      w.write("Blurfl", 0, 5);
      Assert.assertEquals("Length stays the same", clob.length(), origLen);
      w.write("BazBarfoo", 0, 9);
      Assert.assertEquals("Length grown by 9", clob.length(), origLen + 9);
      end = clob.getSubString(origLen - 4, 999);
      Assert.assertEquals("Stream has written bytes correctly", "BlurfBazBarfoo", end);
    } finally {
      rs.close();
    }
  }

  @Test
  public void testConnBlob() throws SQLException {
    // test we can get a blob from the connection and do basic set, get,
    // and search operations on it
    byte[] foobarbaz = "foobarbaz".getBytes();
    byte[] foo = "foo".getBytes();
    byte[] bar = "bar".getBytes();
    byte[] baz = "baz".getBytes();
    byte[] blurfl = "blurfl".getBytes();
    Assert.assertTrue("Connection has blobAsBytea", ((PgConnection) conn).getBlobAsBytea());
    Blob blob = conn.createBlob();
    Assert.assertTrue("PgBlobBytea created", blob instanceof PgBlobBytea);
    Assert.assertEquals("Initial blob has length 0", blob.length(), 0);
    blob.setBytes(1, foobarbaz);
    Assert.assertEquals("Blob has length 9", blob.length(), 9);
    Assert.assertArrayEquals("Blob has contents 'foobarbaz'", blob.getBytes(1, 999), foobarbaz);
    byte[] s = blob.getBytes(4, 3);
    Assert.assertArrayEquals("blob subcontents = 'bar'", s, bar);
    byte[] s2 = blob.getBytes(7, 99);
    Assert.assertEquals("blob remaining contents have length 3", s2.length, 3);
    Assert.assertArrayEquals("blob remaining contents = 'baz'", s2, baz);
    long pos = blob.position(bar, 1);
    Assert.assertEquals("blob position of 'bar' starting at 1 is 4", pos, 4);
    pos = blob.position(bar, 4);
    Assert.assertEquals("blob position of 'bar' starting at 4 is 4", pos, 4);
    pos = blob.position(bar, 7);
    Assert.assertEquals("'bar' not found in blob starting at 7", pos, -1);
    pos = blob.position(blurfl, 4);
    Assert.assertEquals("'blurfl' not found in blob", pos, -1);
    Blob srch = new PgBlobBytea(bar);
    pos = blob.position(srch, 1);
    Assert.assertEquals("blob position of blob 'bar' starting at 1 is 4", pos, 4);
    pos = blob.position(srch, 4);
    Assert.assertEquals("blob position of blob 'bar' starting at 4 is 4", pos, 4);
    pos = blob.position(srch, 7);
    Assert.assertEquals("blob 'bar' not found in blob starting at 7", pos, -1);
    srch.setBytes(1, blurfl);
    pos = blob.position(srch, 4);
    Assert.assertEquals("blob 'blurfl' not found in blob", pos, -1);
    blob.truncate(3);
    Assert.assertEquals("truncated blob length = 3", blob.length(), 3);
    Assert.assertArrayEquals("truncated blob contents = 'foo'", blob.getBytes(1, 999), foo);
    // a few searches to test the KMP algorithm
    srch = new PgBlobBytea("011123".getBytes());
    Assert.assertEquals("1112 is at position 2", srch.position("1112".getBytes(), 1), 2);
    Assert.assertEquals("112 is at position 3", srch.position("112".getBytes(), 1), 3);
    srch = new PgBlobBytea("acfacabacabacacdk".getBytes());
    Assert.assertEquals("acabacacd is at position 8", srch.position("acabacacd".getBytes(), 1), 8);
  }

  @Test
  public void testResultSetBlob() throws SQLException {
    // test we can get a blob from a bytea field in a resultset
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(TestUtil.selectSQL("testlobvarlena", "byteacol"));
    try {
      Assert.assertTrue("Connection has blobAsText", ((PgConnection) conn).getBlobAsBytea());
      Assert.assertTrue("Result set has a row", rs.next());
      Blob blob = rs.getBlob(1);
      Assert.assertTrue("returned result is a PgBlobBytea", blob instanceof PgBlobBytea);
    } finally {
      rs.close();
    }
  }

  @Test
  public void testBlobStatementParams() throws SQLException {
    // test we can set a Blob as a parameter
    QueryExecutor qe = ((PgConnection) conn).getQueryExecutor();
    if (qe.getServerVersionNum() == 90606) {
      // this version apparently has trouble in Travis-CI / oraclejdk8
      return;
    }
    conn.setAutoCommit(false);
    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO testlobvarlena (byteacol) VALUES"
        + " (?)");
    byte[] foo = "foo".getBytes();
    byte[] foobar = "foobar".getBytes();
    pstmt.setObject(1, foo, Types.BLOB);
    pstmt.executeUpdate();
    Blob blob = conn.createBlob();
    blob.setBytes(1, foobar);
    pstmt.setObject(1, blob, Types.BLOB);
    pstmt.executeUpdate();
    conn.rollback();
  }

  @Test
  public void testBlobReaderStreams() throws SQLException, IOException {
    // test streams operations of a Blob
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(TestUtil.selectSQL("testlobvarlena", "byteacol"));
    try {
      rs.next();
      Blob blob = rs.getBlob(1);
      byte[] b = blob.getBytes(1, 99999);
      InputStream is = blob.getBinaryStream();
      byte[] buffer = new byte[b.length];
      int index = 0;
      int c = is.read();
      while (c >= 0) {
        buffer[index++] = (byte) c;
        c = is.read();
      }
      Assert.assertEquals("Input stream from blob has corrrect length", index, b.length);
      Assert.assertArrayEquals("Input stream from blob has correct contents", b, buffer);
    } finally {
      rs.close();
    }
  }

  @Test
  public void testBlobWriterStreams() throws SQLException, IOException {
    // test streams operations of a Blob
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(TestUtil.selectSQL("testlobvarlena", "byteacol"));
    try {
      rs.next();
      Blob blob = rs.getBlob(1);
      int origLen = (int) blob.length();
      OutputStream os = blob.setBinaryStream(origLen - 4);
      byte[] blurfl = "blurfl".getBytes();
      os.write(blurfl, 0, 5);
      Assert.assertEquals("Length stays the same", blob.length(), origLen);
      os.write("bazBarfoo".getBytes());
      Assert.assertEquals("Length grown by 9", blob.length(), origLen + 9);
      byte[] end = blob.getBytes(origLen - 4, 999);
      Assert.assertArrayEquals("Stream has written bytes correctly", end, "blurfbazBarfoo".getBytes());
    } finally {
      rs.close();
    }
  }

  @Test
  public void testResultSetClobLong() throws SQLException, IOException {
    // https://aone.alibaba-inc.com/v2/project/903276/bug/52114671
    Reader input = new FileReader("src/test/resources/test-file.xml");

    PreparedStatement stmt = conn.prepareStatement("select ? from dual");
    stmt.setClob(1, input, 20);
    stmt.execute();
    ResultSet rs = stmt.getResultSet();
    TestUtil.printResultSet(rs);
  }

}
