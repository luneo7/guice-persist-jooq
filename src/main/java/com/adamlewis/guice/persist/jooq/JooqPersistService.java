/*
 * Copyright 2014 Adam L. Lewis
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.adamlewis.guice.persist.jooq;

import java.util.Optional;
import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.persist.PersistService;
import com.google.inject.persist.UnitOfWork;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConnectionProvider;
import org.jooq.tools.jdbc.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Based on the JPA Persistence Service by Dhanji R. Prasanna (dhanji@gmail.com)
 *
 * @author Adam Lewis (github@adamlewis.com)
 */
@Singleton
class JooqPersistService implements Provider<DSLContext>, UnitOfWork, PersistService {

  private static final Logger logger = LoggerFactory.getLogger(JooqPersistService.class);

  private static final ThreadLocal<ThreadLocals> threadFactory = new ThreadLocal<>();
  private final Provider<DataSource> jdbcSource;
  private final SQLDialect sqlDialect;
  private final Settings jooqSettings;
  private final Configuration configuration;

  @Inject
  public JooqPersistService(final Provider<DataSource> jdbcSource, final SQLDialect sqlDialect,
      Optional<Settings> jooqSettings, Optional<Configuration> configuration) {
    this.jdbcSource = jdbcSource;
    this.sqlDialect = sqlDialect;
    this.configuration = configuration.orElse(null);
    if (configuration.isPresent() && jooqSettings.isPresent()) {
      logger.warn("@Injected org.jooq.conf.Settings is being ignored since a full org.jooq.Configuration was supplied");
      this.jooqSettings = null;
    } else {
      this.jooqSettings = jooqSettings.orElse(null);
    }
  }

  @Override
  public DSLContext get() {
    ThreadLocals factory = threadFactory.get();
    if (null == factory) {
      throw new IllegalStateException("Requested Factory outside work unit. "
              + "Try calling UnitOfWork.begin() first, use @Transactional annotation"
              + "or use a PersistFilter if you are inside a servlet environment.");
    }

    return factory.getDSLContext();
  }

  public ThreadLocals getThreadLocals() {
	  return threadFactory.get();
  }

  public boolean isWorking() {
    return threadFactory.get() != null;
  }

  @Override
  public void begin() {
    if (null != threadFactory.get()) {
      throw new IllegalStateException("Work already begun on this thread. "
              + "It looks like you have called UnitOfWork.begin() twice"
              + " without a balancing call to end() in between.");
    }

    threadFactory.set(createThreadLocals());
  }

  private ThreadLocals createThreadLocals() {
    DefaultConnectionProvider conn = null;
    try {
      logger.debug("Getting JDBC connection");
      DataSource dataSource = jdbcSource.get();
      Connection jdbcConn = dataSource.getConnection();
      conn = new DefaultConnectionProvider(jdbcConn);

      DSLContext jooqFactory;

      if (configuration != null) {
        logger.debug("Creating factory from configuration having dialect {}", configuration.dialect());
        jooqFactory = DSL.using(conn, configuration.dialect(), configuration.settings());
      } else {
        if (jooqSettings == null) {
          logger.debug("Creating factory with dialect {}", sqlDialect);
          jooqFactory = DSL.using(conn, sqlDialect);
        } else {
          logger.debug("Creating factory with dialect {} and settings.", sqlDialect);
          jooqFactory = DSL.using(conn, sqlDialect, jooqSettings);
        }
      }
      return new ThreadLocals(jooqFactory, conn);
    } catch (Exception e) {
      if (conn != null) {
        JDBCUtils.safeClose(conn.acquire());
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public void end() {
    ThreadLocals threadLocals = threadFactory.get();
    // Let's not penalize users for calling end() multiple times.
    if (null == threadLocals) {
      return;
    }

    try {
      logger.debug("Closing JDBC connection");
      threadLocals.getConnectionProvider().acquire().close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      threadFactory.remove();
    }
  }


  @Override
  public synchronized void start() {
	  //nothing to do on start
  }

  @Override
  public synchronized void stop() {
	  //nothing to do on stop
  }
}
