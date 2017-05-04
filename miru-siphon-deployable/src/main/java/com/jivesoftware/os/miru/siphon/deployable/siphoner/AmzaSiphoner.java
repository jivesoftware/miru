package com.jivesoftware.os.miru.siphon.deployable.siphoner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.AmzaInterner;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream.TxResult;
import com.jivesoftware.os.amza.api.take.TakeResult;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.query.siphon.MiruSiphonPlugin;
import com.jivesoftware.os.miru.siphon.deployable.MiruSiphonActivityFlusher;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Created by jonathan.colt on 4/27/17.
 */
public class AmzaSiphoner {

    public static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final PartitionProperties CURSOR_PROPERTIES = new PartitionProperties(Durability.fsync_async,
        0, 0, 0, 0, 0, 0, 0, 0,
        false, Consistency.leader_quorum, true, true, false, RowType.primary, "lab", 8, null, -1, -1);

    private static final AmzaSiphonCursor DEFAULT_CURSOR = new AmzaSiphonCursor(Maps.newHashMap());


    private final long additionalSolverAfterNMillis = 10_000; //TODO expose to conf?
    private final long abandonLeaderSolutionAfterNMillis = 30_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 60_000; //TODO expose to conf?

    private final AtomicReference<Callable<Boolean>> runnable = new AtomicReference<>(null);
    public final LongAdder called = new LongAdder();
    public final LongAdder failed = new LongAdder();
    public final LongAdder siphoned = new LongAdder();
    public final LongAdder flushed = new LongAdder();

    public final AmzaInterner amzaInterner = new AmzaInterner();
    public final AmzaSiphonerConfig siphonerConfig;
    private final MiruSiphonPlugin miruSiphonPlugin;
    public final PartitionName partitionName;
    public final String siphonInstancName;
    public final MiruTenantId destinationTenantId;
    public final int batchSize;
    private final PartitionClientProvider partitionClientProvider;
    private final ObjectMapper mapper;

    public AmzaSiphoner(AmzaSiphonerConfig siphonerConfig,
        MiruSiphonPlugin miruSiphonPlugin,
        PartitionName partitionName,
        String siphonInstancName,
        MiruTenantId destinationTenantId,
        int batchSize,
        PartitionClientProvider partitionClientProvider,
        ObjectMapper mapper) {

        this.siphonerConfig = siphonerConfig;
        this.miruSiphonPlugin = miruSiphonPlugin;
        this.partitionName = partitionName;
        this.siphonInstancName = siphonInstancName;
        this.destinationTenantId = destinationTenantId;
        this.batchSize = batchSize;

        this.partitionClientProvider = partitionClientProvider;
        this.mapper = mapper;
    }

    @Override
    public String toString() {
        return "AmzaSiphoner{" +
            "partitionName=" + partitionName +
            ", siphonInstancName='" + siphonInstancName + '\'' +
            ", destinationTenantId=" + destinationTenantId +
            ", batchSize=" + batchSize +
            '}';
    }

    public void stop() {
        runnable.set(() -> false);
    }

    public boolean configHasChanged(AmzaSiphonerConfig siphonerConfig) {
        return false;
    }

    public boolean runnable() {
        return runnable.get() == null;
    }

    public boolean siphon(Callable<Boolean> runnable, MiruSiphonActivityFlusher miruSiphonActivityFlusher) throws Exception {
        if (!this.runnable.compareAndSet(null, runnable)) {
            return true;
        }
        called.increment();

        try {
            String siphonerName = miruSiphonPlugin.name() + "-" + siphonInstancName;
            AmzaSiphonCursor existingCursor = getPartitionCursor(siphonerName, partitionName, DEFAULT_CURSOR);
            PartitionClient partitionClient = partitionClientProvider.getPartition(partitionName);
            Map<RingMember, Long> cursorMemberTxIds = Maps.newHashMap(existingCursor.memberTxIds);

            boolean tookToEnd = false;
            ListMultimap<MiruTenantId, MiruActivity> tenantPartitionedActivites = ArrayListMultimap.create();

            TakeResult takeResult = partitionClient.takeFromTransactionId(null,
                cursorMemberTxIds,
                batchSize,
                highwater -> {
                    if (highwater != null) {
                        for (WALHighwater.RingMemberHighwater memberHighwater : highwater.ringMemberHighwater) {
                            cursorMemberTxIds.merge(memberHighwater.ringMember, memberHighwater.transactionId, Math::max);
                        }
                    }
                },
                (rowTxId, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {

                    ListMultimap<MiruTenantId, MiruActivity> activities = miruSiphonPlugin.siphon(destinationTenantId,
                        rowTxId,
                        prefix,
                        key,
                        value,
                        valueTimestamp,
                        valueTombstoned,
                        valueVersion);

                    siphoned.increment();
                    tenantPartitionedActivites.putAll(activities);
                    return TxResult.MORE;
                },
                additionalSolverAfterNMillis,
                abandonSolutionAfterNMillis,
                Optional.empty());

            if (runnable.call()) {

                for (Entry<MiruTenantId, Collection<MiruActivity>> tenantsActivities : tenantPartitionedActivites.asMap().entrySet()) {
                    miruSiphonActivityFlusher.flushActivities(miruSiphonPlugin, tenantsActivities.getKey(), tenantsActivities.getValue());
                    flushed.add(tenantsActivities.getValue().size());
                }

                cursorMemberTxIds.merge(takeResult.tookFrom, takeResult.lastTxId, Math::max);
                if (takeResult.tookToEnd != null) {
                    for (WALHighwater.RingMemberHighwater ringMemberHighwater : takeResult.tookToEnd.ringMemberHighwater) {
                        cursorMemberTxIds.merge(ringMemberHighwater.ringMember, ringMemberHighwater.transactionId, Math::max);
                    }
                    tookToEnd = true;
                }

                AmzaSiphonCursor cursor = new AmzaSiphonCursor(cursorMemberTxIds);
                if (!existingCursor.equals(cursor)) {
                    savePartitionCursor(siphonerName, partitionName, cursor);
                }
                return tookToEnd;
            } else {
                return true;
            }
        } catch (Throwable t) {
            failed.increment();
            throw t;
        } finally {
            this.runnable.set(null);
        }
    }


    private AmzaSiphonCursor getPartitionCursor(String siphonerName, PartitionName partitionName, AmzaSiphonCursor defaultCursor) throws Exception {
        AmzaSiphonCursor[] cursor = { defaultCursor };
        cursorClient().get(Consistency.leader_quorum,
            null,
            unprefixedWALKeyStream -> {
                return unprefixedWALKeyStream.stream(cursorKey(siphonerName, partitionName));
            }, (prefix, key, value, timestamp, version) -> {

                if (value != null) {
                    cursor[0] = cursorFromValue(value);
                }
                return true;
            }, additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty()
        );
        return cursor[0];
    }

    private void savePartitionCursor(String siphonerName, PartitionName partitionName, AmzaSiphonCursor cursor) throws Exception {
        PartitionClient cursorClient = cursorClient();
        byte[] cursorKey = cursorKey(siphonerName, partitionName);
        byte[] value = valueFromCursor(cursor);
        cursorClient.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> commitKeyValueStream.commit(cursorKey, value, -1, false),
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
    }

    private PartitionClient cursorClient() throws Exception {
        return partitionClientProvider.getPartition(cursorName(), 3, CURSOR_PROPERTIES);
    }

    private PartitionName cursorName() {
        byte[] nameBytes = ("amza-siphoner-cursors-v2").getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, nameBytes, nameBytes);
    }



    private static byte[] valueFromCursor(AmzaSiphonCursor cursor) {
        int valueLength = 1 + 2;
        for (RingMember ringMember : cursor.memberTxIds.keySet()) {
            valueLength += 2 + ringMember.sizeInBytes() + 8;
        }

        byte[] value = new byte[valueLength];
        value[0] = 1; // version
        UIO.unsignedShortBytes(cursor.memberTxIds.size(), value, 1);
        int o = 3;
        for (Entry<RingMember, Long> entry : cursor.memberTxIds.entrySet()) {
            int memberLength = entry.getKey().sizeInBytes();
            UIO.unsignedShortBytes(memberLength, value, o);
            o += 2;
            entry.getKey().toBytes(value, o);
            o += memberLength;
            UIO.longBytes(entry.getValue(), value, o);
            o += 8;
        }
        return value;
    }

    private AmzaSiphonCursor cursorFromValue(byte[] value) throws InterruptedException {
        if (value[0] == 1) {

            int memberTxIdsLength = UIO.bytesUnsignedShort(value, 1);
            int o = 3;

            Map<RingMember, Long> memberTxIds = Maps.newHashMap();
            for (int i = 0; i < memberTxIdsLength; i++) {
                int memberLength = UIO.bytesUnsignedShort(value, o);
                o += 2;
                RingMember member = amzaInterner.internRingMember(value, o, memberLength);
                o += memberLength;
                long txId = UIO.bytesLong(value, o);
                memberTxIds.put(member, txId);
                o += 8;
            }

            return new AmzaSiphonCursor(memberTxIds);
        } else {
            LOG.error("Unsupported cursor version {}", value[0]);
            return null;
        }
    }



    private byte[] cursorKey(String siphonerName, PartitionName partitionName) {
        byte[] siphonerNameBytes = siphonerName.getBytes(StandardCharsets.UTF_8);
        byte[] partitionNameBytes = partitionName.toBytes();
        byte[] key = new byte[1 + 2 + siphonerNameBytes.length + 2 + partitionNameBytes.length];
        int o = 0;
        key[o] = 0; // version
        o++;
        UIO.unsignedShortBytes(siphonerNameBytes.length, key, o);
        o += 2;
        UIO.writeBytes(siphonerNameBytes, key, o);
        o += siphonerNameBytes.length;
        UIO.unsignedShortBytes(partitionNameBytes.length, key, o);
        o += 2;
        UIO.writeBytes(partitionNameBytes, key, o);
        o += partitionNameBytes.length;
        return key;
    }

}
