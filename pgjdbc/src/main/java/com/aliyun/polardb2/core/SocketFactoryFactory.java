/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.aliyun.polardb2.core;

import com.aliyun.polardb2.PGProperty;
import com.aliyun.polardb2.ssl.LibPQFactory;
import com.aliyun.polardb2.util.GT;
import com.aliyun.polardb2.util.ObjectFactory;
import com.aliyun.polardb2.util.PSQLException;
import com.aliyun.polardb2.util.PSQLState;

import java.util.Properties;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

/**
 * Instantiates {@link SocketFactory} based on the {@link PGProperty#SOCKET_FACTORY}.
 */
public class SocketFactoryFactory {

  /**
   * Instantiates {@link SocketFactory} based on the {@link PGProperty#SOCKET_FACTORY}.
   *
   * @param info connection properties
   * @return socket factory
   * @throws PSQLException if something goes wrong
   */
  public static SocketFactory getSocketFactory(Properties info) throws PSQLException {
    // Socket factory
    String socketFactoryClassName = PGProperty.SOCKET_FACTORY.getOrDefault(info);
    if (socketFactoryClassName == null) {
      return SocketFactory.getDefault();
    }
    try {
      return ObjectFactory.instantiate(SocketFactory.class, socketFactoryClassName, info, true,
          PGProperty.SOCKET_FACTORY_ARG.getOrDefault(info));
    } catch (Exception e) {
      throw new PSQLException(
          GT.tr("The SocketFactory class provided {0} could not be instantiated.",
              socketFactoryClassName),
          PSQLState.CONNECTION_FAILURE, e);
    }
  }

  /**
   * Instantiates {@link SSLSocketFactory} based on the {@link PGProperty#SSL_FACTORY}.
   *
   * @param info connection properties
   * @return SSL socket factory
   * @throws PSQLException if something goes wrong
   */
  public static SSLSocketFactory getSslSocketFactory(Properties info) throws PSQLException {
    String classname = PGProperty.SSL_FACTORY.getOrDefault(info);
    if (classname == null
        || "com.aliyun.polardb2.ssl.jdbc4.LibPQFactory".equals(classname)
        || "com.aliyun.polardb2.ssl.LibPQFactory".equals(classname)) {
      return new LibPQFactory(info);
    }
    try {
      return ObjectFactory.instantiate(SSLSocketFactory.class, classname, info, true,
          PGProperty.SSL_FACTORY_ARG.getOrDefault(info));
    } catch (Exception e) {
      throw new PSQLException(
          GT.tr("The SSLSocketFactory class provided {0} could not be instantiated.", classname),
          PSQLState.CONNECTION_FAILURE, e);
    }
  }

}
