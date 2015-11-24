package com.jivesoftware.os.miru.wal.lookup;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.client.AmzaClientProvider.AmzaClient;
import com.jivesoftware.os.amza.shared.AmzaPartitionUpdates;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.AmzaWALUtil;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class AmzaWALLookup implements MiruWALLookup {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final MiruTenantId FULLY_REPAIRED_TENANT = new MiruTenantId(new byte[] { 0 });

    private final AmzaWALUtil amzaWALUtil;
    private final int replicateLookupQuorum;
    private final long replicateLookupTimeoutMillis;

    private final Map<TenantAndPartition, Boolean> knownLookup = Maps.newConcurrentMap();
    private final AtomicBoolean ready = new AtomicBoolean(false);

    public AmzaWALLookup(AmzaWALUtil amzaWALUtil,
        int replicateLookupQuorum,
        long replicateLookupTimeoutMillis) {
        this.amzaWALUtil = amzaWALUtil;
        this.replicateLookupQuorum = replicateLookupQuorum;
        this.replicateLookupTimeoutMillis = replicateLookupTimeoutMillis;
    }

    @Override
    public void add(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        LOG.inc("add>offer");
        knownLookup.computeIfAbsent(new TenantAndPartition(tenantId, partitionId), tenantAndPartition -> {
            try {
                LOG.inc("add>set");
                amzaWALUtil.getLookupTenantsClient().commit(null,
                    new AmzaPartitionUpdates().set(tenantId.getBytes(), null),
                    replicateLookupQuorum, replicateLookupTimeoutMillis, TimeUnit.MILLISECONDS);
                amzaWALUtil.getLookupPartitionsClient().commit(null,
                    new AmzaPartitionUpdates().set(amzaWALUtil.toPartitionsKey(tenantId, partitionId), null),
                    replicateLookupQuorum, replicateLookupTimeoutMillis, TimeUnit.MILLISECONDS);
                return true;
            } catch (Exception e) {
                throw new RuntimeException("Failed to record tenant partition", e);
            }
        });
    }

    @Override
    public void markRepaired() throws Exception {
        LOG.inc("markRepaired");
        amzaWALUtil.getLookupTenantsClient().commit(null,
            new AmzaPartitionUpdates().set(FULLY_REPAIRED_TENANT.getBytes(), FilerIO.longBytes(System.currentTimeMillis())),
            replicateLookupQuorum, replicateLookupTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public List<MiruTenantId> allTenantIds(Callable<Void> repairCallback) throws Exception {
        if (!isReady(repairCallback)) {
            throw new IllegalStateException("Lookup is not ready");
        }

        LOG.inc("allTenantIds");
        AmzaClient client = amzaWALUtil.getLookupTenantsClient();
        final List<MiruTenantId> tenantIds = Lists.newArrayList();
        if (client != null) {
            client.scan(null, null, null, null, (rowTxId, prefix, key, value) -> {
                if (key != null) {
                    MiruTenantId tenantId = new MiruTenantId(key);
                    if (!FULLY_REPAIRED_TENANT.equals(tenantId)) {
                        tenantIds.add(tenantId);
                    }
                }
                return true;
            });
        }
        return tenantIds;
    }

    @Override
    public void allPartitions(PartitionsStream partitionsStream, Callable<Void> repairCallback) throws Exception {
        if (!isReady(repairCallback)) {
            throw new IllegalStateException("Lookup is not ready");
        }

        LOG.inc("allPartitions");
        AmzaClient client = amzaWALUtil.getLookupPartitionsClient();
        if (client != null) {
            client.scan(null, null, null, null, (rowTxId, prefix, key, value) -> {
                if (key != null) {
                    TenantAndPartition tenantAndPartition = amzaWALUtil.fromPartitionsKey(key);
                    if (!partitionsStream.stream(tenantAndPartition.tenantId, tenantAndPartition.partitionId)) {
                        return false;
                    }
                }
                return true;
            });
        }
    }

    @Override
    public void allPartitionsForTenant(MiruTenantId tenantId, PartitionsStream partitionsStream, Callable<Void> repairCallback) throws Exception {
        if (!isReady(repairCallback)) {
            throw new IllegalStateException("Lookup is not ready");
        }

        LOG.inc("allPartitionsForTenant");
        AmzaClient client = amzaWALUtil.getLookupPartitionsClient();
        if (client != null) {
            byte[] fromKey = amzaWALUtil.toPartitionsKey(tenantId, null);
            byte[] toKey = WALKey.prefixUpperExclusive(fromKey);
            client.scan(null, fromKey, null, toKey, (rowTxId, prefix, key, value) -> {
                if (key != null) {
                    TenantAndPartition tenantAndPartition = amzaWALUtil.fromPartitionsKey(key);
                    if (!partitionsStream.stream(tenantAndPartition.tenantId, tenantAndPartition.partitionId)) {
                        return false;
                    }
                }
                return true;
            });
        }
    }

    @Override
    public MiruPartitionId largestPartitionId(MiruTenantId tenantId, Callable<Void> repairCallback) throws Exception {
        if (!isReady(repairCallback)) {
            throw new IllegalStateException("Lookup is not ready");
        }

        LOG.inc("largestPartitionId");
        AmzaClient client = amzaWALUtil.getLookupPartitionsClient();
        if (client != null) {
            byte[] fromKey = amzaWALUtil.toPartitionsKey(tenantId, null);
            byte[] toKey = WALKey.prefixUpperExclusive(fromKey);
            MiruPartitionId[] partitionId = new MiruPartitionId[1];
            client.scan(null, fromKey, null, toKey, (rowTxId, prefix, key, value) -> {
                if (key != null) {
                    TenantAndPartition tenantAndPartition = amzaWALUtil.fromPartitionsKey(key);
                    partitionId[0] = tenantAndPartition.partitionId;
                }
                return true;
            });
            return partitionId[0] != null ? partitionId[0] : MiruPartitionId.of(0);
        }
        return null;
    }

    private boolean isReady(Callable<Void> repairCallback) throws Exception {
        if (!ready.get()) {
            synchronized (ready) {
                if (ready.get()) {
                    return true;
                }

                byte[] value = amzaWALUtil.getLookupTenantsClient().getValue(null, FULLY_REPAIRED_TENANT.getBytes());
                if (value != null) {
                    ready.set(true);
                } else {
                    repairCallback.call();
                    value = amzaWALUtil.getLookupTenantsClient().getValue(null, FULLY_REPAIRED_TENANT.getBytes());
                    if (value != null) {
                        ready.set(true);
                    } else {
                        return false;
                    }
                }
            }
        }
        return true;
    }
}
