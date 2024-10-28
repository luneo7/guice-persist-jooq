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

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.persist.Transactional;
import com.google.inject.persist.UnitOfWork;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.jooq.impl.DefaultConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Adam Lewis
 */
class JdbcLocalTxnInterceptor implements MethodInterceptor {
  private static final Logger logger = LoggerFactory.getLogger(JdbcLocalTxnInterceptor.class);
  private static final ConcurrentMap<Method, Transactional> transactionalMethods = new ConcurrentHashMap<Method, Transactional>();

  private final Provider<JooqPersistService> jooqPersistServiceProvider;
  private final Provider<UnitOfWork> unitOfWorkProvider;

  @Transactional
  private static class Internal {
  }

  @Inject
  public JdbcLocalTxnInterceptor(Provider<JooqPersistService> jooqPersistServiceProvider,
                                 Provider<UnitOfWork> unitOfWorkProvider) {
    this.jooqPersistServiceProvider = jooqPersistServiceProvider;
    this.unitOfWorkProvider = unitOfWorkProvider;
  }

  @Override
  public Object invoke(final MethodInvocation methodInvocation) throws Throwable {
    UnitOfWork unitOfWork = unitOfWorkProvider.get();
    JooqPersistService jooqProvider = jooqPersistServiceProvider.get();

    // Should we start a unit of work?
    boolean isWorking = jooqProvider.isWorking();
    if (!isWorking) {
      unitOfWork.begin();
    }

    // Allow 'joining' of transactions if there is an enclosing @Transactional method.
    if (isWorking) {
      return methodInvocation.proceed();
    }

    DefaultConnectionProvider conn = jooqProvider.getThreadLocals().getConnectionProvider();
    boolean reenableAutoCommit = false;

    try {
      if (conn.getAutoCommit()) {
        logger.debug("Disabling JDBC auto commit for this thread");
        reenableAutoCommit = true;
        conn.setAutoCommit(false);
      }

      Object result = methodInvocation.proceed();

      logger.debug("Committing JDBC transaction");
      conn.commit();

      return result;
    } catch (Exception e) {
      //commit transaction only if rollback didn't occur
      if (rollbackIfNecessary(readTransactionMetadata(methodInvocation), e, conn)) {
        logger.debug("Committing JDBC transaction");
        conn.commit();
      }
      //propagate whatever exception is thrown anyway
      throw e;
    } finally {
      // Close the em if necessary (guarded so this code doesn't run unless catch fired).
        if (reenableAutoCommit) {
          try {
           conn.setAutoCommit(true);
          } catch (Exception ignored) {
          }
        }
        unitOfWork.end();
    }
  }

  private Transactional readTransactionMetadata(final MethodInvocation methodInvocation) {
    Method method = methodInvocation.getMethod();
    Transactional cachedTransactional = transactionalMethods.get(method);
    if (cachedTransactional != null) {
      return cachedTransactional;
    }

    return transactionalMethods.computeIfAbsent(method, ignored -> {
        Transactional transactional;
        transactional = method.getAnnotation(Transactional.class);
        if (null == transactional) {
            // If none on method, try the class.
            Class<?> targetClass = methodInvocation.getThis().getClass();
            transactional = targetClass.getAnnotation(Transactional.class);
        }
        if (null == transactional) {
            // If there is no transactional annotation present, use the default
            transactional = Internal.class.getAnnotation(Transactional.class);
        }
        return transactional;
    });
  }

  /**
   * Returns True if rollback DID NOT HAPPEN (i.e. if commit should continue).
   *
   * @param transactional The metadata annotation of the method
   * @param e             The exception to test for rollback
   * @param conn          A DefaultConnectionProvider to issue rollbacks on
   */
  private boolean rollbackIfNecessary(final Transactional transactional,
                                      final Exception e,
                                      final DefaultConnectionProvider conn) {
    boolean commit = true;

    //check rollback clauses
    for (Class<? extends Exception> rollBackOn : transactional.rollbackOn()) {

      //if one matched, try to perform a rollback
      if (rollBackOn.isInstance(e)) {
        commit = false;

        //check ignore clauses (supercedes rollback clause)
        for (Class<? extends Exception> exceptOn : transactional.ignore()) {
          //An exception to the rollback clause was found, DON'T rollback
          // (i.e. commit and throw anyway)
          if (exceptOn.isInstance(e)) {
            commit = true;
            break;
          }
        }

        //rollback only if nothing matched the ignore check
        if (!commit) {
          logger.debug("Rolling back JDBC transaction for this thread");
          conn.rollback();
        }
        //otherwise continue to commit

        break;
      }
    }

    return commit;
  }
}
