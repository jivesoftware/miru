package com.jivesoftware.os.miru.plugin;

import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruJustInTimeBackfillerizer;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;

/**
 *
 */
public interface MiruProvider<T extends Miru> {

    MiruStats getStats();

    T getMiru(MiruTenantId tenantId);

    MiruActivityInternExtern getActivityInternExtern(MiruTenantId tenantId);

    MiruJustInTimeBackfillerizer getBackfillerizer(MiruTenantId tenantId);

    MiruTermComposer getTermComposer();

    <R extends MiruRemotePartition<?, ?, ?>> R getRemotePartition(Class<R> remotePartitionClass);
}
