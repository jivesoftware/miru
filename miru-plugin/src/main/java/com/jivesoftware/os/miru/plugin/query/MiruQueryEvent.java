package com.jivesoftware.os.miru.plugin.query;

import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 * Created by jonathan.colt on 5/4/17.
 */
public interface MiruQueryEvent {

    void event(MiruTenantId tenantId, MiruActorId actorId, String family, String destination, long latency, String... verbs);
}
