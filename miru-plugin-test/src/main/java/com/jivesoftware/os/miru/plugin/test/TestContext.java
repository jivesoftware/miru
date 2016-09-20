package com.jivesoftware.os.miru.plugin.test;

import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndexProvider;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import org.roaringbitmap.RoaringBitmap;

/**
 *
 */
public class TestContext<BM extends IBM, IBM> implements MiruRequestContext<BM, IBM, AmzaSipCursor> {

    private final MiruSchema schema;
    private final MiruTermComposer termComposer;
    private final MiruFieldIndexProvider<BM, IBM> fieldIndexProvider;

    public TestContext(MiruSchema schema, MiruTermComposer termComposer, MiruFieldIndexProvider<BM, IBM> fieldIndexProvider) {
        this.schema = schema;
        this.termComposer = termComposer;
        this.fieldIndexProvider = fieldIndexProvider;
    }

    @Override
    public MiruSchema getSchema() {
        return schema;
    }

    @Override
    public MiruTermComposer getTermComposer() {
        return termComposer;
    }

    @Override
    public MiruTimeIndex getTimeIndex() {
        return null;
    }

    @Override
    public MiruActivityIndex getActivityIndex() {
        return null;
    }

    @Override
    public MiruFieldIndexProvider<BM, IBM> getFieldIndexProvider() {
        return fieldIndexProvider;
    }

    @Override
    public MiruSipIndex<AmzaSipCursor> getSipIndex() {
        return null;
    }

    @Override
    public MiruPluginCacheProvider getCacheProvider() {
        return null;
    }

    @Override
    public MiruAuthzIndex<BM, IBM> getAuthzIndex() {
        return null;
    }

    @Override
    public MiruRemovalIndex<BM, IBM> getRemovalIndex() {
        return null;
    }

    @Override
    public MiruUnreadTrackingIndex<BM, IBM> getUnreadTrackingIndex() {
        return null;
    }

    @Override
    public MiruInboxIndex<BM, IBM> getInboxIndex() {
        return null;
    }

    @Override
    public StripingLocksProvider<MiruStreamId> getStreamLocks() {
        return null;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public boolean hasChunkStores() {
        return false;
    }

    @Override
    public boolean hasLabIndex() {
        return false;
    }
}
