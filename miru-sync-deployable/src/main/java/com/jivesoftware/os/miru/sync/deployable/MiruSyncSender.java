package com.jivesoftware.os.miru.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.client.aquarium.AmzaClientAquariumProvider;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.jive.utils.ordered.id.IdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaUnvailableException;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.sync.MiruSyncClient;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient.PartitionRange;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus.WriterCount;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.StreamBatch;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.sync.api.MiruSyncSenderConfig;
import com.jivesoftware.os.miru.sync.api.MiruSyncTenantConfig;
import com.jivesoftware.os.miru.sync.api.MiruSyncTenantTuple;
import com.jivesoftware.os.miru.sync.api.MiruSyncTimeShiftStrategy;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.mutable.MutableLong;

import static com.jivesoftware.os.miru.sync.deployable.MiruSyncSender.ProgressType.forward;
import static com.jivesoftware.os.miru.sync.deployable.MiruSyncSender.ProgressType.initial;
import static com.jivesoftware.os.miru.sync.deployable.MiruSyncSender.ProgressType.reverse;

/**
 *
 */
public class MiruSyncSender<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final PartitionProperties PROGRESS_PROPERTIES = new PartitionProperties(Durability.fsync_async,
        0, 0, 0, 0, 0, 0, 0, 0,
        false, Consistency.leader_quorum, true, true, false, RowType.primary, "lab", 8, null, -1, -1);

    private static final PartitionProperties CURSOR_PROPERTIES = new PartitionProperties(Durability.fsync_async,
        0, 0, 0, 0, 0, 0, 0, 0,
        false, Consistency.leader_quorum, true, true, false, RowType.primary, "lab", 8, null, -1, -1);

    private final MiruStats stats;
    private final MiruSyncSenderConfig config;
    private final AmzaClientAquariumProvider amzaClientAquariumProvider;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final IdPacker idPacker;
    private final int syncRingStripes;
    private final ScheduledExecutorService executorService;
    private final Future[] syncFutures;
    private final MiruSchemaProvider schemaProvider;
    private final MiruClusterClient clusterClient;
    private final MiruWALClient<C, S> fromWALClient;
    private final MiruSyncClient toSyncClient;
    private final PartitionClientProvider partitionClientProvider;
    private final ObjectMapper mapper;
    private final MiruSyncConfigProvider syncConfigProvider;
    private final C defaultCursor;
    private final Class<C> cursorClass;

    private final Set<TenantTuplePartition> maxAgeSet = Collections.newSetFromMap(Maps.newConcurrentMap());
    private final Set<TenantTuplePartition> forwardClosedSet = Collections.newSetFromMap(Maps.newConcurrentMap());
    private final Set<TenantTuplePartition> forwardIgnoreSet = Collections.newSetFromMap(Maps.newConcurrentMap());
    private final Set<MiruSyncTenantTuple> ensuredTenantIds = Collections.newSetFromMap(Maps.newConcurrentMap());
    private final SetMultimap<MiruTenantId, MiruTenantId> registeredSchemas = Multimaps.synchronizedSetMultimap(HashMultimap.create());
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final long additionalSolverAfterNMillis = 10_000; //TODO expose to conf?
    private final long abandonLeaderSolutionAfterNMillis = 30_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 60_000; //TODO expose to conf?

    public MiruSyncSender(MiruStats stats,
        MiruSyncSenderConfig config,
        AmzaClientAquariumProvider amzaClientAquariumProvider,
        TimestampedOrderIdProvider orderIdProvider,
        IdPacker idPacker,
        int syncRingStripes,
        ScheduledExecutorService executorService,
        MiruSchemaProvider schemaProvider,
        MiruClusterClient clusterClient,
        MiruWALClient<C, S> fromWALClient,
        MiruSyncClient toSyncClient,
        PartitionClientProvider partitionClientProvider,
        ObjectMapper mapper,
        MiruSyncConfigProvider syncConfigProvider,
        C defaultCursor,
        Class<C> cursorClass) {

        this.stats = stats;
        this.config = config;
        this.amzaClientAquariumProvider = amzaClientAquariumProvider;
        this.orderIdProvider = orderIdProvider;
        this.idPacker = idPacker;
        this.syncRingStripes = syncRingStripes;
        this.executorService = executorService;
        this.syncFutures = new ScheduledFuture[syncRingStripes];
        this.schemaProvider = schemaProvider;
        this.clusterClient = clusterClient;
        this.fromWALClient = fromWALClient;
        this.toSyncClient = toSyncClient;
        this.partitionClientProvider = partitionClientProvider;
        this.mapper = mapper;
        this.syncConfigProvider = syncConfigProvider;
        this.defaultCursor = defaultCursor;
        this.cursorClass = cursorClass;
    }

    public MiruSyncSenderConfig getConfig() {
        return config;
    }

    public boolean configHasChanged(MiruSyncSenderConfig senderConfig) {
        return !config.equals(senderConfig);
    }

    private String aquariumName(int syncStripe) {
        return "miru-sync-" + config.name + "-stripe-" + syncStripe;
    }

    private class SyncRunnable implements Runnable {

        private final int stripe;

        public SyncRunnable(int stripe) {
            this.stripe = stripe;
        }

        @Override
        public void run() {
            SyncResult syncResult = null;
            try {
                syncResult = syncStripe(stripe);
            } catch (InterruptedException e) {
                LOG.info("Sync thread {} was interrupted", stripe);
            } catch (Throwable t) {
                LOG.error("Failure in sync thread {}", new Object[] { stripe }, t);
            } finally {
                synchronized (syncFutures) {
                    if (syncFutures[stripe] == null) {
                        LOG.info("Sync runnable {} was canceled", stripe);
                    } else {
                        long delay = syncResult != null && syncResult.advanced ? 0 : config.syncIntervalMillis;
                        syncFutures[stripe] = executorService.schedule(this, delay, TimeUnit.MILLISECONDS);
                    }
                }
            }
        }
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            for (int i = 0; i < syncRingStripes; i++) {
                amzaClientAquariumProvider.register(aquariumName(i));
            }

            for (int i = 0; i < syncRingStripes; i++) {
                syncFutures[i] = executorService.submit(new SyncRunnable(i));
            }
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            for (int i = 0; i < syncRingStripes; i++) {
                synchronized (syncFutures) {
                    syncFutures[i].cancel(true);
                    syncFutures[i] = null;
                }
            }
        }
    }

    public interface ProgressStream {
        boolean stream(MiruTenantId fromTenantId,
            MiruTenantId toTenantId,
            ProgressType type,
            int partitionId,
            long timestamp,
            boolean taking) throws Exception;
    }

    public void streamProgress(MiruTenantId fromTenantId, MiruTenantId toTenantId, ProgressStream stream) throws Exception {
        PartitionClient progressClient = progressClient();
        byte[] fromKey = fromTenantId == null ? null : progressKey(fromTenantId, toTenantId, null);
        byte[] toKey = fromTenantId == null ? null : WALKey.prefixUpperExclusive(fromKey);
        progressClient.scan(Consistency.leader_quorum, true,
            prefixedKeyRangeStream -> {
                return prefixedKeyRangeStream.stream(null, fromKey, null, toKey);
            },
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    MiruTenantId from = progressKeyFromTenantId(key);
                    MiruTenantId to = progressKeyToTenantId(key);
                    ProgressType type = progressType(key);
                    if (type != null) {
                        int partitionId = partitionIdFromProgressValue(value);
                        boolean taking = takingFromProgressValue(value);
                        return stream.stream(from, to, type, partitionId, timestamp, taking);
                    } else {
                        LOG.warn("Encountered unknown progress type for key:{}", Arrays.toString(key));
                    }
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
    }

    public boolean resetProgress(MiruTenantId tenantId) throws Exception {
        registeredSchemas.removeAll(tenantId);

        PartitionClient cursorClient = cursorClient();
        List<byte[]> cursorKeys = Lists.newArrayList();
        cursorClient.scan(Consistency.leader_quorum, false,
            prefixedKeyRangeStream -> {
                byte[] fromCursorKey = tenantPartitionCursorKey(tenantId, null, null);
                byte[] toCursorKey = WALKey.prefixUpperExclusive(fromCursorKey);
                prefixedKeyRangeStream.stream(null, fromCursorKey, null, toCursorKey);

                byte[] fromStateKey = tenantPartitionStateKey(tenantId, null, null);
                byte[] toStateKey = WALKey.prefixUpperExclusive(fromStateKey);
                prefixedKeyRangeStream.stream(null, fromCursorKey, null, toStateKey);

                return true;
            },
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    cursorKeys.add(key);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
        if (!cursorKeys.isEmpty()) {
            cursorClient.commit(Consistency.leader_quorum, null,
                commitKeyValueStream -> {
                    for (byte[] cursorKey : cursorKeys) {
                        commitKeyValueStream.commit(cursorKey, null, -1, true);
                    }
                    return true;
                },
                additionalSolverAfterNMillis,
                abandonSolutionAfterNMillis,
                Optional.empty());
        }

        PartitionClient progressClient = progressClient();
        byte[] fromProgressKey = progressKey(tenantId, null, null);
        byte[] toProgressKey = WALKey.prefixUpperExclusive(fromProgressKey);
        List<byte[]> progressKeys = Lists.newArrayList();
        progressClient.scan(Consistency.leader_quorum, false,
            prefixedKeyRangeStream -> prefixedKeyRangeStream.stream(null, fromProgressKey, null, toProgressKey),
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    progressKeys.add(key);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
        if (!progressKeys.isEmpty()) {
            progressClient.commit(Consistency.leader_quorum, null,
                commitKeyValueStream -> {
                    for (byte[] progressKey : progressKeys) {
                        commitKeyValueStream.commit(progressKey, null, -1, true);
                    }
                    return true;
                },
                additionalSolverAfterNMillis,
                abandonSolutionAfterNMillis,
                Optional.empty());
        }

        LOG.info("Reset progress for tenant:{} cursors:{} progress:{}", tenantId, cursorKeys.size(), progressKeys.size());
        return true;
    }

    private boolean isElected(int syncStripe) throws Exception {
        LivelyEndState livelyEndState = livelyEndState(syncStripe);
        return livelyEndState != null && livelyEndState.isOnline() && livelyEndState.getCurrentState() == State.leader;
    }

    private LivelyEndState livelyEndState(int syncStripe) throws Exception {
        return amzaClientAquariumProvider.livelyEndState(aquariumName(syncStripe));
    }

    private PartitionClient progressClient() throws Exception {
        return partitionClientProvider.getPartition(progressName(), 3, PROGRESS_PROPERTIES);
    }

    private PartitionName progressName() {
        byte[] nameBytes = ("miru-sync-progress-" + config.name).getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, nameBytes, nameBytes);
    }

    private PartitionClient cursorClient() throws Exception {
        return partitionClientProvider.getPartition(cursorName(), 3, CURSOR_PROPERTIES);
    }

    private PartitionName cursorName() {
        byte[] nameBytes = ("miru-sync-cursor-" + config.name).getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, nameBytes, nameBytes);
    }

    private SyncResult syncStripe(int stripe) throws Exception {
        if (!isElected(stripe)) {
            return new SyncResult(0, 0, 0, false);
        }

        LOG.info("Syncing stripe:{}", stripe);
        int tenantCount = 0;
        long count = 0;
        long skipped = 0;
        long ignored = 0;
        boolean progress = false;
        Map<MiruSyncTenantTuple, MiruSyncTenantConfig> tenantTupleConfigs = syncConfigProvider.getAll(config.name);
        for (Entry<MiruSyncTenantTuple, MiruSyncTenantConfig> entry : tenantTupleConfigs.entrySet()) {
            if (!isElected(stripe)) {
                break;
            }
            MiruSyncTenantTuple tenantTuple = entry.getKey();
            int tenantStripe = Math.abs(tenantTuple.from.hashCode() % syncRingStripes);
            if (tenantStripe == stripe) {
                tenantCount++;
                try {
                    ensureSchema(tenantTuple.from, tenantTuple.to);
                } catch (MiruSchemaUnvailableException e) {
                    LOG.warn("Sync skipped tenantId:{} because schema is unavailable", tenantTuple.from);
                    continue;
                }
                if (!isElected(stripe)) {
                    break;
                }

                boolean ensured = ensureTenantPartitionState(tenantTuple, entry.getValue(), stripe);
                if (!isElected(stripe)) {
                    break;
                }
                if (!ensured) {
                    continue;
                }

                SyncResult syncResult = syncTenant(tenantTuple, entry.getValue(), stripe, forward);
                if (syncResult.count > 0) {
                    LOG.info("Synced stripe:{} tenantId:{} activities:{} skipped:{} ignored:{} type:{}",
                        stripe, tenantTuple.from, syncResult.count, syncResult.skipped, syncResult.ignored, forward);
                }
                count += syncResult.count;
                skipped += syncResult.skipped;
                ignored += syncResult.ignored;
                progress |= syncResult.advanced;

                if (!isElected(stripe)) {
                    break;
                }

                syncResult = syncTenant(tenantTuple, entry.getValue(), stripe, reverse);
                if (syncResult.count > 0) {
                    LOG.info("Synced stripe:{} tenantId:{} activities:{} skipped:{} ignored:{} type:{}",
                        stripe, tenantTuple.from, syncResult.count, syncResult.skipped, syncResult.ignored, reverse);
                }
                count += syncResult.count;
                skipped += syncResult.skipped;
                ignored += syncResult.ignored;
                progress |= syncResult.advanced;
            }
        }
        LOG.info("Synced stripe:{} tenants:{} activities:{} skipped:{} ignored:{}", stripe, tenantCount, count, skipped, ignored);
        // Reverse count implies the consumption of at least one partition, which we'll use to tighten the sync interval.
        // Forward count is more relaxed to achieve better batching.
        return new SyncResult(count, skipped, ignored, progress);
    }

    private void ensureSchema(MiruTenantId fromTenantId, MiruTenantId toTenantId) throws Exception {
        if (!registeredSchemas.containsEntry(fromTenantId, toTenantId)) {
            MiruSchema schema = schemaProvider.getSchema(fromTenantId);

            LOG.info("Submitting schema fromTenantId:{} toTenantId:{}", fromTenantId, toTenantId);
            toSyncClient.registerSchema(toTenantId, schema);
            registeredSchemas.put(fromTenantId, toTenantId);
        }
    }

    private boolean ensureTenantPartitionState(MiruSyncTenantTuple tenantTuple, MiruSyncTenantConfig tenantConfig, int stripe) throws Exception {
        if (ensuredTenantIds.contains(tenantTuple)) {
            return true;
        }
        if (tenantConfig.timeShiftStrategy == MiruSyncTimeShiftStrategy.step) {
            MiruPartitionId largestPartitionId = fromWALClient.getLargestPartitionId(tenantTuple.from);
            if (!isElected(stripe)) {
                return false;
            }

            List<MiruPartitionId> partitionIds = Lists.newArrayList();
            for (MiruPartitionId partitionId = largestPartitionId; partitionId != null; partitionId = partitionId.prev()) {
                PartitionRange range = clusterClient.getIngressRange(tenantTuple.from, partitionId);
                if (range != null && range.rangeMinMax.clockMin > 0 && range.rangeMinMax.clockMax > 0) {
                    if (tenantConfig.startTimestampMillis > 0 && range.rangeMinMax.clockMax < tenantConfig.startTimestampMillis) {
                        LOG.info("Initialization of step state found terminal range from:{} to:{} partitionId:{} clockMax:{} startTimestamp:{}",
                            tenantTuple.from, tenantTuple.to, partitionId, range.rangeMinMax.clockMax, tenantConfig.startTimestampMillis);
                        break;
                    } else if (tenantConfig.stopTimestampMillis <= 0 || range.rangeMinMax.clockMin <= tenantConfig.stopTimestampMillis) {
                        partitionIds.add(partitionId);
                    }
                } else {
                    MiruActivityWALStatus status = fromWALClient.getActivityWALStatusForTenant(tenantTuple.from, partitionId);
                    if (!isElected(stripe)) {
                        return false;
                    }
                    if (status == null) {
                        LOG.warn("Failed to initialize step state due to missing status from:{} to:{} partitionId:{}",
                            tenantTuple.from, tenantTuple.to, partitionId);
                        return false;
                    }
                    long maxClockTimestamp = 0;
                    for (WriterCount count : status.counts) {
                        maxClockTimestamp = Math.max(maxClockTimestamp, count.clockTimestamp);
                    }
                    if (tenantConfig.startTimestampMillis > 0 && maxClockTimestamp < tenantConfig.startTimestampMillis || maxClockTimestamp == 0) {
                        LOG.info("Initialization of step state found terminal status from:{} to:{} partitionId:{} clockMax:{} startTimestamp:{}",
                            tenantTuple.from, tenantTuple.to, partitionId, maxClockTimestamp, tenantConfig.startTimestampMillis);
                        break;
                    } else {
                        partitionIds.add(partitionId);
                    }
                }
            }

            Collections.reverse(partitionIds);

            int partitionCount = partitionIds.size();
            long timeShiftRangeMillis = tenantConfig.timeShiftStopTimestampMillis - tenantConfig.timeShiftStartTimestampMillis;
            long rangePerPartitionMillis = timeShiftRangeMillis / partitionCount;
            long startTimestampMillis = tenantConfig.timeShiftStartTimestampMillis;

            for (MiruPartitionId partitionId : partitionIds) {
                TenantPartitionState state = getTenantPartitionState(tenantTuple.from, tenantTuple.to, partitionId);
                if (state != null) {
                    LOG.info("Skipped step state from:{} to:{} partitionId:{} because it was already initialized",
                        tenantTuple.from, tenantTuple.to, partitionId);
                } else {
                    long count = fromWALClient.getActivityCount(tenantTuple.from, partitionId);
                    long timeShiftOrderIds = count == 0 ? 1 : idPacker.pack(rangePerPartitionMillis, 0, 0) / count;
                    long stopTimestampMillis = startTimestampMillis + rangePerPartitionMillis;
                    long startTimestampOrderId = orderIdProvider.getApproximateId(startTimestampMillis);
                    long stopTimestampOrderId = orderIdProvider.getApproximateId(stopTimestampMillis);
                    LOG.info("Initialized step state from:{} to:{} partitionId:{} count:{} timeShift:{}" +
                            " startTimestamp:{} stopTimestamp:{} startOrderId:{} stopOrderId:{}",
                        tenantTuple.from, tenantTuple.to, partitionId, count, timeShiftOrderIds,
                        startTimestampMillis, stopTimestampMillis, startTimestampOrderId, stopTimestampOrderId);
                    state = new TenantPartitionState(timeShiftOrderIds, startTimestampOrderId, stopTimestampOrderId);
                    saveTenantPartitionCursor(tenantTuple, partitionId, null, state);
                }
                startTimestampMillis += rangePerPartitionMillis;
            }
        }
        ensuredTenantIds.add(tenantTuple);
        return true;
    }

    private SyncResult syncTenant(MiruSyncTenantTuple tenantTuple, MiruSyncTenantConfig tenantConfig, int stripe, ProgressType type) throws Exception {
        TenantProgress progress = getTenantProgress(tenantTuple.from, tenantTuple.to, stripe);
        if (!isElected(stripe)) {
            return new SyncResult(0, 0, 0, false);
        }
        if (type == reverse && progress.reversePartitionId == null) {
            // all done
            return new SyncResult(0, 0, 0, false);
        }

        MiruPartitionId partitionId = type == reverse ? progress.reversePartitionId : progress.forwardPartitionId;
        TenantTuplePartition tenantTuplePartition = new TenantTuplePartition(tenantTuple, partitionId);
        MiruActivityWALStatus status = fromWALClient.getActivityWALStatusForTenant(tenantTuple.from, partitionId);
        if (status == null) {
            LOG.warn("Failed to get status from:{} to:{} partition:{}", tenantTuple.from, tenantTuple.to, partitionId);
            return new SyncResult(0, 0, 0, false);
        }
        if (!isElected(stripe)) {
            return new SyncResult(0, 0, 0, false);
        }
        if (type == reverse) {
            long maxClockTimestamp = -1;
            for (WriterCount count : status.counts) {
                maxClockTimestamp = Math.max(maxClockTimestamp, count.clockTimestamp);
            }
            if (tenantConfig.startTimestampMillis != -1 && (maxClockTimestamp == -1 || maxClockTimestamp < tenantConfig.startTimestampMillis)) {
                if (!maxAgeSet.contains(tenantTuplePartition)) {
                    LOG.info("Reverse sync reached max age from:{} to:{} partition:{} clock:{}",
                        tenantTuple.from, tenantTuple.to, partitionId, maxClockTimestamp);
                    setTenantProgress(tenantTuple, partitionId, type, false);
                    maxAgeSet.add(tenantTuplePartition);
                }
                return new SyncResult(0, 0, 0, false);
            } else if (status.ends.containsAll(status.begins)) {
                PartitionRange partitionRange = clusterClient.getIngressRange(tenantTuple.from, partitionId);
                return syncTenantPartition(tenantTuple, tenantConfig, stripe, partitionId, type, status, partitionRange, progress.reverseTaking);
            } else {
                LOG.error("Reverse sync encountered open partition from:{} to:{} partition:{}", tenantTuple.from, tenantTuple.to, partitionId);
                return new SyncResult(0, 0, 0, false);
            }
        } else if (forwardClosedSet.contains(tenantTuplePartition) || forwardIgnoreSet.contains(tenantTuplePartition)) {
            return new SyncResult(0, 0, 0, false);
        } else {
            return syncTenantPartition(tenantTuple, tenantConfig, stripe, partitionId, type, status, null, progress.forwardTaking);
        }
    }

    private static class SyncResult {
        private final long count;
        private final long skipped;
        private final long ignored;
        private final boolean advanced;

        public SyncResult(long count, long skipped, long ignored, boolean advanced) {
            this.count = count;
            this.skipped = skipped;
            this.ignored = ignored;
            this.advanced = advanced;
        }
    }

    private SyncResult syncTenantPartition(MiruSyncTenantTuple tenantTuple,
        MiruSyncTenantConfig tenantConfig,
        int stripe,
        MiruPartitionId partitionId,
        ProgressType type,
        MiruActivityWALStatus status,
        PartitionRange partitionRange,
        boolean taking) throws Exception {

        if (!isElected(stripe)) {
            return new SyncResult(0, 0, 0, false);
        }

        if (status.begins.isEmpty()) {
            LOG.warn("Source partition has no open writers, progress will not advance from:{} to:{} partition:{} type:{}",
                tenantTuple.from, tenantTuple.to, partitionId, type);
        }

        TenantTuplePartition tenantTuplePartition = new TenantTuplePartition(tenantTuple, partitionId);
        boolean closed = !status.begins.isEmpty() && status.ends.containsAll(status.begins);
        int numWriters = Math.max(status.begins.size(), 1);
        int lastIndex = -1;

        long timeShift = 0;
        long clockShift = 0;
        long nextActivityTimestamp = 0;
        TenantPartitionState state = null;
        if (tenantConfig.timeShiftStrategy == MiruSyncTimeShiftStrategy.linear && tenantConfig.timeShiftMillis > 0) {
            timeShift = idPacker.pack(tenantConfig.timeShiftMillis, 0, 0); // packing with zeroes should preserve writerId, orderId on shift
            clockShift = tenantConfig.timeShiftMillis;
        } else if (tenantConfig.timeShiftStrategy == MiruSyncTimeShiftStrategy.step) {
            state = getTenantPartitionState(tenantTuple.from, tenantTuple.to, partitionId);
            if (state != null) {
                timeShift = state.timeShiftOrderIds;
                nextActivityTimestamp = state.timeShiftStartTimestampOrderId;
                long nextClockTimestamp = idPacker.unpack(nextActivityTimestamp)[0] + JiveEpochTimestampProvider.JIVE_EPOCH;
                LOG.info("Sync step with state from:{} to:{} partitionId:{} type:{} timeShift:{} nextActivityTime:{} nextClockTime:{}",
                    tenantTuple.from, tenantTuple.to, partitionId, type, timeShift, nextActivityTimestamp, nextClockTimestamp);
            } else {
                if (type == forward) {
                    setTenantProgress(tenantTuple, partitionId, type, false);
                    if (forwardIgnoreSet.add(tenantTuplePartition)) {
                        LOG.info("Forward progress is missing step state from:{} to:{} partition:{} type:{}",
                            tenantTuple.from, tenantTuple.to, partitionId, type);
                    }
                } else {
                    LOG.info("Reverse sync skipped missing step state from:{} to:{} partition:{} type:{}", tenantTuple.from, tenantTuple.to, partitionId, type);
                    advanceTenantProgress(tenantTuple, partitionId, type);
                }
                return new SyncResult(0, 0, 0, false);
            }
        }

        String readableFromTo = tenantTuple.from.toString() + "/" + tenantTuple.to.toString();
        String statsBytes = "sender/sync/" + readableFromTo + "/bytes";
        String statsCount = "sender/sync/" + readableFromTo + "/count";

        AtomicLong clockTimestamp = new AtomicLong(-1);
        MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory(clockTimestamp::get);

        C cursor = getTenantPartitionCursor(tenantTuple.from, tenantTuple.to, partitionId);
        if (cursor == null) {
            toSyncClient.writeActivity(tenantTuple.to, partitionId, Collections.singletonList(
                partitionedActivityFactory.begin(-1, partitionId, tenantTuple.to, 0)));
            cursor = defaultCursor;
        }

        boolean intersects = true;
        if (tenantConfig.timeShiftStrategy == MiruSyncTimeShiftStrategy.step && state == null) {
            intersects = false;
        } else if (partitionRange != null && partitionRange.rangeMinMax.clockMin > 0 && partitionRange.rangeMinMax.clockMax > 0) {
            if (tenantConfig.startTimestampMillis > 0 && tenantConfig.startTimestampMillis > partitionRange.rangeMinMax.clockMax
                || tenantConfig.stopTimestampMillis > 0 && tenantConfig.stopTimestampMillis < partitionRange.rangeMinMax.clockMin) {
                LOG.info("No intersection from:{} to:{} partition:{} type:{} clockMin:{} clockMax:{} start:{} stop:{}",
                    tenantTuple.from, tenantTuple.to, partitionId, type,
                    partitionRange.rangeMinMax.clockMin, partitionRange.rangeMinMax.clockMax,
                    tenantConfig.startTimestampMillis, tenantConfig.stopTimestampMillis);
                intersects = false;
            }
        }

        boolean took = false;
        int synced = 0;
        int skipped = 0;
        int ignored = 0;
        while (intersects) {
            MutableLong bytesCount = new MutableLong();
            long stopAtTimestamp = type == forward ? orderIdProvider.getApproximateId(System.currentTimeMillis() - config.forwardSyncDelayMillis) : -1;
            long start = System.currentTimeMillis();
            StreamBatch<MiruWALEntry, C> batch = fromWALClient.getActivity(tenantTuple.from,
                partitionId,
                cursor,
                config.batchSize,
                stopAtTimestamp,
                bytesCount);
            if (!isElected(stripe)) {
                return new SyncResult(synced, skipped, ignored, false);
            }
            int activityTypes = 0;
            if (batch.activities != null && !batch.activities.isEmpty()) {
                long ingressLatency = System.currentTimeMillis() - start;
                stats.ingressed("sender/sync/bytes", bytesCount.longValue(), 0);
                stats.ingressed(statsBytes, bytesCount.longValue(), ingressLatency);
                stats.ingressed("sender/sync/count", batch.activities.size(), 0);
                stats.ingressed(statsCount, batch.activities.size(), ingressLatency);
                start = System.currentTimeMillis();

                List<MiruPartitionedActivity> activities = Lists.newArrayListWithCapacity(batch.activities.size());
                for (MiruWALEntry entry : batch.activities) {
                    if (entry.activity.type.isActivityType() && entry.activity.activity.isPresent()) {
                        activityTypes++;
                        if ((tenantConfig.startTimestampMillis == -1 || entry.activity.clockTimestamp > tenantConfig.startTimestampMillis)
                            && (tenantConfig.stopTimestampMillis == -1 || entry.activity.clockTimestamp < tenantConfig.stopTimestampMillis)) {

                            MiruActivity fromActivity = entry.activity.activity.get();
                            long forgeClockTimestamp, forgeActivityTimestamp, startActivityTimestamp, stopActivityTimestamp;
                            if (tenantConfig.timeShiftStrategy == MiruSyncTimeShiftStrategy.step) {
                                forgeClockTimestamp = idPacker.unpack(nextActivityTimestamp)[0] + JiveEpochTimestampProvider.JIVE_EPOCH;
                                forgeActivityTimestamp = nextActivityTimestamp;
                                nextActivityTimestamp = forgeActivityTimestamp + timeShift;
                                startActivityTimestamp = state.timeShiftStartTimestampOrderId;
                                stopActivityTimestamp = state.timeShiftStopTimestampOrderId;
                            } else {
                                forgeClockTimestamp = entry.activity.clockTimestamp + clockShift;
                                forgeActivityTimestamp = fromActivity.time + timeShift;
                                //TODO correct start/stop for linear shift
                                startActivityTimestamp = 0;
                                stopActivityTimestamp = 0;
                            }

                            if ((startActivityTimestamp <= 0 || forgeActivityTimestamp >= startActivityTimestamp)
                                && (stopActivityTimestamp <= 0 || forgeActivityTimestamp < stopActivityTimestamp)) {
                                // copy to tenantId with time shift, and assign fake writerId
                                MiruActivity toActivity = fromActivity.copyTo(tenantTuple.to, forgeActivityTimestamp);
                                clockTimestamp.set(forgeClockTimestamp);

                                //TODO rough estimate of index depth, wildly inaccurate if we skip most of a partition
                                lastIndex = Math.max(lastIndex, numWriters * entry.activity.index);
                                activities.add(partitionedActivityFactory.activity(-1,
                                    partitionId,
                                    lastIndex,
                                    toActivity));
                            }
                        } else {
                            skipped++;
                        }
                    } else {
                        ignored++;
                    }
                }
                if (!activities.isEmpty()) {
                    // mimic writer by injecting begin activity with fake writerId
                    activities.add(partitionedActivityFactory.begin(-1, partitionId, tenantTuple.to, lastIndex));
                    toSyncClient.writeActivity(tenantTuple.to, partitionId, activities);
                    synced += activities.size();

                    long egressLatency = System.currentTimeMillis() - start;
                    stats.egressed("sender/sync/bytes", bytesCount.longValue(), 0);
                    stats.egressed(statsBytes, bytesCount.longValue(), egressLatency);
                    stats.egressed("sender/sync/count", activities.size(), 0);
                    stats.egressed(statsCount, activities.size(), egressLatency);

                    if (!took) {
                        // set 'taking' to true to indicate active progress
                        setTenantProgress(tenantTuple, partitionId, type, true);
                        took = true;
                    }
                }
            }
            if (state != null) {
                state = new TenantPartitionState(state.timeShiftOrderIds, nextActivityTimestamp, state.timeShiftStopTimestampOrderId);
            }
            saveTenantPartitionCursor(tenantTuple, partitionId, batch.cursor, state);
            cursor = batch.cursor;
            if (activityTypes == 0) {
                break;
            }
        }

        // if it closed while syncing, we'll catch it next time
        if (closed) {
            if (clockTimestamp.get() == -1) {
                // no sync timestamp to share, must use current time
                clockTimestamp.set(System.currentTimeMillis());
            }

            // close our fake writerId
            toSyncClient.writeActivity(tenantTuple.to, partitionId, Collections.singletonList(
                partitionedActivityFactory.end(-1, partitionId, tenantTuple.to, lastIndex)));
            advanceTenantProgress(tenantTuple, partitionId, type);
        } else {
            if (type == forward && tenantConfig.closed && forwardClosedSet.add(tenantTuplePartition)) {
                LOG.info("Forward sync is closed from:{} to:{} partition:{}", tenantTuple.from, tenantTuple.to, partitionId);
                // close our fake writerId
                toSyncClient.writeActivity(tenantTuple.to, partitionId, Collections.singletonList(
                    partitionedActivityFactory.end(-1, partitionId, tenantTuple.from, lastIndex)));
                setTenantProgress(tenantTuple, partitionId, type, false);
            } else if (took) {
                // took at least one thing and caught up, so set 'taking' back to false
                setTenantProgress(tenantTuple, partitionId, type, false);
            } else if (taking) {
                // progress indicated previously taking, so set 'taking' back to false
                setTenantProgress(tenantTuple, partitionId, type, false);
            }
        }
        return new SyncResult(synced, skipped, ignored, closed);
    }

    private void advanceTenantProgress(MiruSyncTenantTuple tenantTuple, MiruPartitionId partitionId, ProgressType type) throws Exception {
        MiruPartitionId advanced = (type == reverse) ? partitionId.prev() : partitionId.next();
        PartitionClient partitionClient = progressClient();
        boolean taking = advanced != null;
        byte[] progressKey = progressKey(tenantTuple.from, tenantTuple.to, type);
        byte[] value = progressValue(advanced == null ? -1 : advanced.getId(), taking);
        partitionClient.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> commitKeyValueStream.commit(progressKey, value, -1, false),
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
        LOG.info("Tenant progress fromTenantId:{} toTenantId:{} type:{} advanced from:{} to:{} taking:{}",
            tenantTuple.from, tenantTuple.to, type, partitionId, advanced, taking);
    }

    private void setTenantProgress(MiruSyncTenantTuple tenantTuple, MiruPartitionId partitionId, ProgressType type, boolean taking) throws Exception {
        PartitionClient partitionClient = progressClient();
        byte[] progressKey = progressKey(tenantTuple.from, tenantTuple.to, type);
        byte[] value = progressValue(partitionId == null ? -1 : partitionId.getId(), taking);
        partitionClient.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> commitKeyValueStream.commit(progressKey, value, -1, false),
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
        LOG.info("Tenant progress fromTenantId:{} toTenantId:{} type:{} partitionId:{} set taking:{}",
            tenantTuple.from, tenantTuple.to, type, partitionId, taking);
    }

    /**
     * @return null if finished, empty if not started, else the last synced partition
     */
    private TenantProgress getTenantProgress(MiruTenantId fromTenantId, MiruTenantId toTenantId, int stripe) throws Exception {
        int[] progressId = { Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE };
        Boolean[] progressTaking = { null, null, null };
        streamProgress(fromTenantId, toTenantId, (fromTenantId1, toTenantId1, type, partitionId, timestamp, taking) -> {
            progressId[type.index] = partitionId;
            progressTaking[type.index] = taking;
            return true;
        });

        if (progressId[initial.index] == Integer.MIN_VALUE) {
            MiruPartitionId largestPartitionId = fromWALClient.getLargestPartitionId(fromTenantId);
            MiruPartitionId prevPartitionId = largestPartitionId == null ? null : largestPartitionId.prev();
            progressId[initial.index] = largestPartitionId == null ? 0 : largestPartitionId.getId();
            progressId[reverse.index] = prevPartitionId == null ? -1 : prevPartitionId.getId();
            progressId[forward.index] = largestPartitionId == null ? 0 : largestPartitionId.getId();

            if (!isElected(stripe)) {
                throw new IllegalStateException("Lost leadership while initializing progress fromTenantId:" + fromTenantId
                    + " toTenantId:" + toTenantId + " stripe:" + stripe);
            }

            PartitionClient progressClient = progressClient();
            progressClient.commit(Consistency.leader_quorum, null,
                commitKeyValueStream -> {
                    commitKeyValueStream.commit(progressKey(fromTenantId, toTenantId, initial), progressValue(progressId[initial.index], false), -1, false);
                    commitKeyValueStream.commit(progressKey(fromTenantId, toTenantId, reverse), progressValue(progressId[reverse.index], true), -1, false);
                    commitKeyValueStream.commit(progressKey(fromTenantId, toTenantId, forward), progressValue(progressId[forward.index], true), -1, false);
                    return true;
                },
                additionalSolverAfterNMillis,
                abandonSolutionAfterNMillis,
                Optional.empty());
            LOG.info("Initialized progress fromTenantId:{} toTenantId:{} initial:{} reverse:{} forward:{}",
                fromTenantId, toTenantId, progressId[initial.index], progressId[reverse.index], progressId[forward.index]);
        } else {
            LOG.info("Found progress fromTenantId:{} toTenantId:{} initial:{} reverse:{}/{} forward:{}/{}",
                fromTenantId, toTenantId, progressId[initial.index],
                progressId[reverse.index], progressTaking[reverse.index],
                progressId[forward.index], progressTaking[forward.index]);
            if (progressId[reverse.index] == -1 && progressTaking[reverse.index]) {
                LOG.info("Marking terminal reverse progress fromTenantId:{} toTenantId:{}", fromTenantId, toTenantId);
                setTenantProgress(new MiruSyncTenantTuple(fromTenantId, toTenantId), null, reverse, false);
            }
        }

        MiruPartitionId initialPartitionId = progressId[initial.index] == -1 ? null : MiruPartitionId.of(progressId[initial.index]);
        MiruPartitionId reversePartitionId = progressId[reverse.index] == -1 ? null : MiruPartitionId.of(progressId[reverse.index]);
        boolean reverseTaking = progressTaking[reverse.index] != null && progressTaking[reverse.index];
        MiruPartitionId forwardPartitionId = progressId[forward.index] == -1 ? null : MiruPartitionId.of(progressId[forward.index]);
        boolean forwardTaking = progressTaking[forward.index] != null && progressTaking[forward.index];

        return new TenantProgress(initialPartitionId, reversePartitionId, reverseTaking, forwardPartitionId, forwardTaking);
    }

    public C getTenantPartitionCursor(MiruTenantId fromTenantId, MiruTenantId toTenantId, MiruPartitionId partitionId) throws Exception {
        PartitionClient cursorClient = cursorClient();
        byte[] cursorKey = tenantPartitionCursorKey(fromTenantId, toTenantId, partitionId);
        Object[] result = new Object[1];
        cursorClient.get(Consistency.leader_quorum, null,
            unprefixedWALKeyStream -> unprefixedWALKeyStream.stream(cursorKey),
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    result[0] = mapper.readValue(value, cursorClass);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
        return (C) result[0];
    }

    public TenantPartitionState getTenantPartitionState(MiruTenantId fromTenantId, MiruTenantId toTenantId, MiruPartitionId partitionId) throws Exception {
        PartitionClient cursorClient = cursorClient();
        byte[] stateKey = tenantPartitionStateKey(fromTenantId, toTenantId, partitionId);
        TenantPartitionState[] result = new TenantPartitionState[1];
        cursorClient.get(Consistency.leader_quorum, null,
            unprefixedWALKeyStream -> unprefixedWALKeyStream.stream(stateKey),
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    result[0] = mapper.readValue(value, TenantPartitionState.class);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
        return result[0];
    }

    private void saveTenantPartitionCursor(MiruSyncTenantTuple tenantTuple,
        MiruPartitionId partitionId,
        C cursor,
        TenantPartitionState state) throws Exception {

        PartitionClient cursorClient = cursorClient();
        cursorClient.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> {
                if (cursor != null) {
                    byte[] cursorKey = tenantPartitionCursorKey(tenantTuple.from, tenantTuple.to, partitionId);
                    byte[] cursorValue = mapper.writeValueAsBytes(cursor);
                    commitKeyValueStream.commit(cursorKey, cursorValue, -1, false);
                }

                if (state != null) {
                    byte[] stateKey = tenantPartitionStateKey(tenantTuple.from, tenantTuple.to, partitionId);
                    byte[] stateValue = mapper.writeValueAsBytes(state);
                    commitKeyValueStream.commit(stateKey, stateValue, -1, false);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
    }

    private MiruTenantId progressKeyFromTenantId(byte[] key) {
        int fromTenantLength = UIO.bytesUnsignedShort(key, 0);
        byte[] fromTenantBytes = new byte[fromTenantLength];
        UIO.readBytes(key, 2, fromTenantBytes);
        return new MiruTenantId(fromTenantBytes);
    }

    private MiruTenantId progressKeyToTenantId(byte[] key) {
        int fromTenantLength = UIO.bytesUnsignedShort(key, 0);
        int toTenantLength = UIO.bytesUnsignedShort(key, 2 + fromTenantLength);
        byte[] toTenantBytes = new byte[toTenantLength];
        UIO.readBytes(key, 2 + fromTenantLength + 2, toTenantBytes);
        return new MiruTenantId(toTenantBytes);
    }

    private ProgressType progressType(byte[] key) {
        return ProgressType.fromIndex(key[key.length - 1]);
    }

    private byte[] progressKey(MiruTenantId fromTenantId, MiruTenantId toTenantId, ProgressType type) {
        if (toTenantId == null) {
            byte[] tenantBytes = fromTenantId.getBytes();
            byte[] key = new byte[2 + tenantBytes.length];
            UIO.unsignedShortBytes(tenantBytes.length, key, 0);
            UIO.writeBytes(tenantBytes, key, 2);
            return key;
        } else if (type == null) {
            byte[] fromTenantBytes = fromTenantId.getBytes();
            byte[] toTenantBytes = toTenantId.getBytes();
            byte[] key = new byte[2 + fromTenantBytes.length + 2 + toTenantBytes.length];
            UIO.unsignedShortBytes(fromTenantBytes.length, key, 0);
            UIO.writeBytes(fromTenantBytes, key, 2);
            UIO.unsignedShortBytes(toTenantBytes.length, key, 2 + fromTenantBytes.length);
            UIO.writeBytes(toTenantBytes, key, 2 + fromTenantBytes.length + 2);
            return key;
        } else {
            byte[] fromTenantBytes = fromTenantId.getBytes();
            byte[] toTenantBytes = toTenantId.getBytes();
            byte[] key = new byte[2 + fromTenantBytes.length + 2 + toTenantBytes.length + 1];
            UIO.unsignedShortBytes(fromTenantBytes.length, key, 0);
            UIO.writeBytes(fromTenantBytes, key, 2);
            UIO.unsignedShortBytes(toTenantBytes.length, key, 2 + fromTenantBytes.length);
            UIO.writeBytes(toTenantBytes, key, 2 + fromTenantBytes.length + 2);
            key[2 + fromTenantBytes.length + 2 + toTenantBytes.length] = type.index;
            return key;
        }
    }

    private byte[] progressValue(int partitionId, boolean taking) {
        byte[] value = new byte[5];
        UIO.intBytes(partitionId, value, 0);
        value[4] = (byte) (taking ? 1 : 0);
        return value;
    }

    private int partitionIdFromProgressValue(byte[] value) {
        return UIO.bytesInt(value);
    }

    private boolean takingFromProgressValue(byte[] value) {
        return (value.length >= 5 && value[4] == 1);
    }

    private byte[] tenantPartitionCursorKey(MiruTenantId fromTenantId, MiruTenantId toTenantId, MiruPartitionId partitionId) {
        byte[] fromTenantBytes = fromTenantId.getBytes();
        if (fromTenantBytes.length >= CURSOR_RESERVED_OFFSET) {
            throw new IllegalArgumentException("Source tenant is too long");
        }
        if (toTenantId == null) {
            byte[] key = new byte[2 + fromTenantBytes.length];
            UIO.unsignedShortBytes(fromTenantBytes.length, key, 0);
            UIO.writeBytes(fromTenantBytes, key, 2);
            return key;
        } else if (partitionId == null) {
            byte[] toTenantBytes = toTenantId.getBytes();
            byte[] key = new byte[2 + fromTenantBytes.length + 2 + toTenantBytes.length];
            UIO.unsignedShortBytes(fromTenantBytes.length, key, 0);
            UIO.writeBytes(fromTenantBytes, key, 2);
            UIO.unsignedShortBytes(toTenantBytes.length, key, 2 + fromTenantBytes.length);
            UIO.writeBytes(toTenantBytes, key, 2 + fromTenantBytes.length + 2);
            return key;
        } else {
            byte[] toTenantBytes = toTenantId.getBytes();
            byte[] key = new byte[2 + fromTenantBytes.length + 2 + toTenantBytes.length + 4];
            UIO.unsignedShortBytes(fromTenantBytes.length, key, 0);
            UIO.writeBytes(fromTenantBytes, key, 2);
            UIO.unsignedShortBytes(toTenantBytes.length, key, 2 + fromTenantBytes.length);
            UIO.writeBytes(toTenantBytes, key, 2 + fromTenantBytes.length + 2);
            UIO.intBytes(partitionId.getId(), key, 2 + fromTenantBytes.length + 2 + toTenantBytes.length);
            return key;
        }
    }

    private static final int CURSOR_RESERVED_OFFSET = 32_768;
    private static final int STATE_PREFIX = CURSOR_RESERVED_OFFSET;
    //private static final int OTHER_PREFIX = CURSOR_RESERVED_OFFSET + 1;

    private byte[] tenantPartitionStateKey(MiruTenantId fromTenantId, MiruTenantId toTenantId, MiruPartitionId partitionId) {
        byte[] fromTenantBytes = fromTenantId.getBytes();
        if (fromTenantBytes.length >= CURSOR_RESERVED_OFFSET) {
            throw new IllegalArgumentException("Source tenant is too long");
        }
        if (toTenantId == null) {
            byte[] key = new byte[2 + 2 + fromTenantBytes.length];
            UIO.unsignedShortBytes(STATE_PREFIX, key, 0);
            UIO.unsignedShortBytes(fromTenantBytes.length, key, 2);
            UIO.writeBytes(fromTenantBytes, key, 2 + 2);
            return key;
        } else if (partitionId == null) {
            byte[] toTenantBytes = toTenantId.getBytes();
            byte[] key = new byte[2 + 2 + fromTenantBytes.length + 2 + toTenantBytes.length];
            UIO.unsignedShortBytes(STATE_PREFIX, key, 0);
            UIO.unsignedShortBytes(fromTenantBytes.length, key, 2);
            UIO.writeBytes(fromTenantBytes, key, 2 + 2);
            UIO.unsignedShortBytes(toTenantBytes.length, key, 2 + 2 + fromTenantBytes.length);
            UIO.writeBytes(toTenantBytes, key, 2 + 2 + fromTenantBytes.length + 2);
            return key;
        } else {
            byte[] toTenantBytes = toTenantId.getBytes();
            byte[] key = new byte[2 + 2 + fromTenantBytes.length + 2 + toTenantBytes.length + 4];
            UIO.unsignedShortBytes(STATE_PREFIX, key, 0);
            UIO.unsignedShortBytes(fromTenantBytes.length, key, 2);
            UIO.writeBytes(fromTenantBytes, key, 2 + 2);
            UIO.unsignedShortBytes(toTenantBytes.length, key, 2 + 2 + fromTenantBytes.length);
            UIO.writeBytes(toTenantBytes, key, 2 + 2 + fromTenantBytes.length + 2);
            UIO.intBytes(partitionId.getId(), key, 2 + 2 + fromTenantBytes.length + 2 + toTenantBytes.length);
            return key;
        }
    }

    public enum ProgressType {
        initial((byte) 0),
        reverse((byte) 1),
        forward((byte) 2);

        public final byte index;

        ProgressType(byte index) {
            this.index = index;
        }

        public static ProgressType fromIndex(byte index) {
            for (ProgressType type : values()) {
                if (type.index == index) {
                    return type;
                }
            }
            return null;
        }
    }

    private static class TenantProgress {
        private final MiruPartitionId initialPartitionId;
        private final MiruPartitionId reversePartitionId;
        private final boolean reverseTaking;
        private final MiruPartitionId forwardPartitionId;
        private final boolean forwardTaking;

        public TenantProgress(MiruPartitionId initialPartitionId,
            MiruPartitionId reversePartitionId,
            boolean reverseTaking,
            MiruPartitionId forwardPartitionId,
            boolean forwardTaking) {
            this.initialPartitionId = initialPartitionId;
            this.reversePartitionId = reversePartitionId;
            this.reverseTaking = reverseTaking;
            this.forwardPartitionId = forwardPartitionId;
            this.forwardTaking = forwardTaking;
        }
    }

    private static class TenantTuplePartition {
        private final MiruSyncTenantTuple tuple;
        private final MiruPartitionId partitionId;

        public TenantTuplePartition(MiruSyncTenantTuple tuple, MiruPartitionId partitionId) {
            this.tuple = tuple;
            this.partitionId = partitionId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TenantTuplePartition that = (TenantTuplePartition) o;

            if (tuple != null ? !tuple.equals(that.tuple) : that.tuple != null) {
                return false;
            }
            return partitionId != null ? partitionId.equals(that.partitionId) : that.partitionId == null;

        }

        @Override
        public int hashCode() {
            int result = tuple != null ? tuple.hashCode() : 0;
            result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
            return result;
        }
    }
}
