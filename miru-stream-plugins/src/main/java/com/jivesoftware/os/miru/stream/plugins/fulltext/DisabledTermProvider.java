package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class DisabledTermProvider implements FullTextTermProvider {

    @Override
    public boolean isEnabled(MiruTenantId tenantId) {
        return false;
    }

    @Override
    public Collection<String> getSupportedSchemaNames() {
        return Collections.emptyList();
    }

    @Override
    public MiruFilter getAcceptableFilter() {
        return MiruFilter.NO_FILTER;
    }

    @Override
    public String[] getFieldNames() {
        return new String[0];
    }

    @Override
    public boolean gatherText(MiruPartitionCoord coord, int[] indexes, Map<String, MiruValue[][]> fieldValues, GatherStream gatherStream) throws Exception {
        return true;
    }
}
