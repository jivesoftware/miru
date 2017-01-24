package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class DisabledTermProviderInitializer implements FullTextTermProviderInitializer<DisabledTermProvider> {

    @Override
    public DisabledTermProvider initialize(MiruProvider<? extends Miru> miruProvider) throws Exception {
        return new DisabledTermProvider();
    }
}
