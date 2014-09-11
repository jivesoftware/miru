package com.jivesoftware.os.miru.service;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.query.index.MiruJustInTimeBackfillerizer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class MiruBackfillerizerInitializer {

    public MiruLifecyle<MiruJustInTimeBackfillerizer> initialize(final MiruServiceConfig config, MiruHost miruHost) {

        String readStreamIdsPropName = config.getReadStreamIdsPropName();
        if (readStreamIdsPropName != null && readStreamIdsPropName.isEmpty()) {
            readStreamIdsPropName = null;
        }

        final ExecutorService backfillExecutor = Executors.newFixedThreadPool(10); //TODO expose to config
        final MiruJustInTimeBackfillerizer backfillerizer = new MiruJustInTimeBackfillerizer(
                miruHost, Optional.fromNullable(readStreamIdsPropName), backfillExecutor);

        return new MiruLifecyle<MiruJustInTimeBackfillerizer>() {

            @Override
            public MiruJustInTimeBackfillerizer getService() {
                return backfillerizer;
            }

            @Override
            public void start() throws Exception {
            }

            @Override
            public void stop() throws Exception {
                backfillExecutor.shutdownNow();
            }
        };
    }

}
