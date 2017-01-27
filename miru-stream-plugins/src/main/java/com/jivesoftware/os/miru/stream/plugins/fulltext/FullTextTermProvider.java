package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import java.util.Collection;
import java.util.Map;

/**
 *
 */
public interface FullTextTermProvider {

    boolean isEnabled(MiruTenantId tenantId);

    boolean gatherText(MiruPartitionCoord coord, int[] indexes, Map<String, MiruValue[][]> fieldValues, GatherStream gatherStream) throws Exception;

    Collection<String> getSupportedSchemaNames();

    MiruFilter getAcceptableFilter();

    String[] getFieldNames();

    String[] getIndexFieldNames();

    interface GatherStream {
        boolean stream(String fieldName, MiruValue value, int[] ids) throws Exception;
    }
}
