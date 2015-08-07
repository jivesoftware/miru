package com.jivesoftware.os.miru.wal.lookup;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.MiruVoidByte;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.api.KeyedColumnValueCallbackStream;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class RCVSWALLookup implements MiruWALLookup {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final MiruTenantId FULLY_REPAIRED_TENANT = new MiruTenantId(new byte[] { 0 });

    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruPartitionId, Long, ? extends Exception> walLookupTable;
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final Map<TenantAndPartition, Boolean> knownLookup = Maps.newConcurrentMap();

    public RCVSWALLookup(RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruPartitionId, Long, ? extends Exception> walLookupTable) {
        this.walLookupTable = walLookupTable;
    }

    @Override
    public void add(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        LOG.inc("add>offer");
        knownLookup.computeIfAbsent(new TenantAndPartition(tenantId, partitionId), tenantAndPartition -> {
            LOG.inc("add>set");
            try {
                walLookupTable.add(MiruVoidByte.INSTANCE, tenantId, partitionId, System.currentTimeMillis(), null, null);
            } catch (Exception e) {
                throw new RuntimeException("Failed to record tenant partition", e);
            }
            return true;
        });
    }

    @Override
    public void markRepaired() throws Exception {
        LOG.inc("markRepaired");
        walLookupTable.add(MiruVoidByte.INSTANCE, FULLY_REPAIRED_TENANT, MiruPartitionId.of(0), System.currentTimeMillis(), null, null);
    }

    @Override
    public List<MiruTenantId> allTenantIds(Callable<Void> repairCallback) throws Exception {
        if (!isReady(repairCallback)) {
            throw new IllegalArgumentException("Lookup is not ready");
        }

        LOG.inc("allTenantIds");
        return internalTenantIds();
    }

    @Override
    public void allPartitions(PartitionsStream partitionsStream, Callable<Void> repairCallback) throws Exception {
        if (!isReady(repairCallback)) {
            throw new IllegalArgumentException("Lookup is not ready");
        }

        LOG.inc("allPartitions");
        List<KeyedColumnValueCallbackStream<MiruTenantId, MiruPartitionId, Long, Object>> rowKeyCallbackStreamPairs = Lists.newArrayList();
        List<MiruTenantId> tenantIds = internalTenantIds();
        for (MiruTenantId tenantId : tenantIds) {
            rowKeyCallbackStreamPairs.add(new KeyedColumnValueCallbackStream<>(tenantId, cvat -> {
                if (cvat != null) {
                    if (!partitionsStream.stream(tenantId, cvat.getColumn())) {
                        return null;
                    }
                }
                return cvat;
            }));
        }
        walLookupTable.multiRowGetAll(MiruVoidByte.INSTANCE, rowKeyCallbackStreamPairs);
    }

    @Override
    public void allPartitionsForTenant(MiruTenantId tenantId, PartitionsStream partitionsStream, Callable<Void> repairCallback) throws Exception {
        if (!isReady(repairCallback)) {
            throw new IllegalArgumentException("Lookup is not ready");
        }

        LOG.inc("allPartitionsForTenant");
        walLookupTable.getEntrys(MiruVoidByte.INSTANCE, tenantId, null, null, 1_000, false, null, null, columnValueAndTimestamp -> {
            if (columnValueAndTimestamp != null) {
                if (!partitionsStream.stream(tenantId, columnValueAndTimestamp.getColumn())) {
                    return null;
                }
            }
            return columnValueAndTimestamp;
        });
    }

    @Override
    public MiruPartitionId largestPartitionId(MiruTenantId tenantId, Callable<Void> repairCallback) throws Exception {
        if (!isReady(repairCallback)) {
            throw new IllegalArgumentException("Lookup is not ready");
        }

        LOG.inc("largestPartitionId");
        MiruPartitionId[] partitionId = new MiruPartitionId[1];
        walLookupTable.getEntrys(MiruVoidByte.INSTANCE, tenantId, null, null, 1_000, false, null, null, columnValueAndTimestamp -> {
            if (columnValueAndTimestamp != null) {
                partitionId[0] = columnValueAndTimestamp.getColumn();
            }
            return columnValueAndTimestamp;
        });
        return partitionId[0];
    }

    private List<MiruTenantId> internalTenantIds() throws Exception {
        final List<MiruTenantId> tenantIds = Lists.newArrayList();
        walLookupTable.getAllRowKeys(10_000, null, r -> {
            if (r != null && !FULLY_REPAIRED_TENANT.equals(r.getRow())) {
                tenantIds.add(r.getRow());
            }
            return r;
        });
        return tenantIds;
    }

    private boolean isReady(Callable<Void> repairCallback) throws Exception {
        if (!ready.get()) {
            synchronized (ready) {
                if (ready.get()) {
                    return true;
                }

                boolean[] found = new boolean[1];
                walLookupTable.getValues(MiruVoidByte.INSTANCE, FULLY_REPAIRED_TENANT, null, 1L, 1, false, null, null, value -> {
                    if (value != null) {
                        found[0] = true;
                    }
                    return null;
                });
                if (found[0]) {
                    ready.set(true);
                } else {
                    repairCallback.call();
                    walLookupTable.getValues(MiruVoidByte.INSTANCE, FULLY_REPAIRED_TENANT, null, 1L, 1, false, null, null, value -> {
                        if (value != null) {
                            found[0] = true;
                        }
                        return null;
                    });
                    if (found[0]) {
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
