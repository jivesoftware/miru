package com.jivesoftware.os.miru.query.siphon;

import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 * Created by jonathan.colt on 4/27/17.
 */
public interface MiruSiphonPlugin {

    String name();

    MiruSchema schema(MiruTenantId tenantId) throws Exception;

    ListMultimap<MiruTenantId, MiruActivity> siphon(MiruTenantId tenantId,
        long rowTxId,
        byte[] prefix,
        byte[] key,
        byte[] value,
        long valueTimestamp,
        boolean valueTombstoned,
        long valueVersion) throws Exception;
}
