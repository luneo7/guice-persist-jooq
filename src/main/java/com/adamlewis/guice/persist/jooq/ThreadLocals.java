package com.adamlewis.guice.persist.jooq;

import org.jooq.DSLContext;
import org.jooq.impl.DefaultConnectionProvider;

final class ThreadLocals {
    private final DSLContext dslContext;
    private final DefaultConnectionProvider connectionProvider;

    ThreadLocals(DSLContext dslContext, DefaultConnectionProvider connectionProvider) {
        this.dslContext = dslContext;
        this.connectionProvider = connectionProvider;
    }

    DSLContext getDSLContext() {
        return dslContext;
    }

    DefaultConnectionProvider getConnectionProvider() {
        return connectionProvider;
    }
}
