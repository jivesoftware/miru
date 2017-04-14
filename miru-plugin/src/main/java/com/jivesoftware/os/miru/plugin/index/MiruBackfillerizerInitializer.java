package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.plugin.backfill.MiruInboxReadTracker;
import com.jivesoftware.os.miru.plugin.backfill.MiruJustInTimeBackfillerizer;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang.StringUtils;

public class MiruBackfillerizerInitializer {

    public MiruLifecyle<MiruJustInTimeBackfillerizer> initialize(ExecutorService backfillExecutor,
        String readStreamIdsPropName,
        MiruInboxReadTracker inboxReadTracker,
        Set<MiruStreamId> verboseStreamIds) {

        if (StringUtils.isEmpty(readStreamIdsPropName)) {
            readStreamIdsPropName = null;
        }

        final MiruJustInTimeBackfillerizer backfillerizer = new MiruJustInTimeBackfillerizer(inboxReadTracker,
            Optional.fromNullable(readStreamIdsPropName), backfillExecutor, verboseStreamIds);

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
