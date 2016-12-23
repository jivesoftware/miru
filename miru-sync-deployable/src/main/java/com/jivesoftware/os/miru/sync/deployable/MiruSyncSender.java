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
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.sync.MiruSyncClient;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus.WriterCount;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.StreamBatch;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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

    private final AmzaClientAquariumProvider amzaClientAquariumProvider;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final int syncRingStripes;
    private final ExecutorService executorService;
    private final int syncThreadCount;
    private final long syncIntervalMillis;
    private final MiruSchemaProvider schemaProvider;
    private final MiruWALClient<C, S> fromWALClient;
    private final MiruSyncClient toSyncClient;
    private final PartitionClientProvider partitionClientProvider;
    private final ObjectMapper mapper;
    private final MiruSyncConfigStorage whitelistConfigStorage;
    private final int batchSize;
    private final long forwardSyncDelayMillis;
    private final long reverseSyncMaxAgeMillis;
    private final C defaultCursor;
    private final Class<C> cursorClass;

    private final Set<TenantAndPartition> maxAgeSet = Collections.newSetFromMap(Maps.newConcurrentMap());
    private final SetMultimap<MiruTenantId, MiruTenantId> registeredSchemas = Multimaps.synchronizedSetMultimap(HashMultimap.create());
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final long additionalSolverAfterNMillis = 10_000; //TODO expose to conf?
    private final long abandonLeaderSolutionAfterNMillis = 30_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 60_000; //TODO expose to conf?

    public MiruSyncSender(AmzaClientAquariumProvider amzaClientAquariumProvider,
        TimestampedOrderIdProvider orderIdProvider,
        int syncRingStripes,
        ExecutorService executorService,
        int syncThreadCount,
        long syncIntervalMillis,
        MiruSchemaProvider schemaProvider,
        MiruWALClient<C, S> fromWALClient,
        MiruSyncClient toSyncClient,
        PartitionClientProvider partitionClientProvider,
        ObjectMapper mapper,
        MiruSyncConfigStorage whitelistConfigStorage,
        int batchSize,
        long forwardSyncDelayMillis,
        long reverseSyncMaxAgeMillis,
        C defaultCursor,
        Class<C> cursorClass) {

        this.amzaClientAquariumProvider = amzaClientAquariumProvider;
        this.orderIdProvider = orderIdProvider;
        this.syncRingStripes = syncRingStripes;
        this.executorService = executorService;
        this.syncThreadCount = syncThreadCount;
        this.syncIntervalMillis = syncIntervalMillis;
        this.schemaProvider = schemaProvider;
        this.fromWALClient = fromWALClient;
        this.toSyncClient = toSyncClient;
        this.partitionClientProvider = partitionClientProvider;
        this.mapper = mapper;
        this.whitelistConfigStorage = whitelistConfigStorage;
        this.batchSize = batchSize;
        this.forwardSyncDelayMillis = forwardSyncDelayMillis;
        this.reverseSyncMaxAgeMillis = reverseSyncMaxAgeMillis;
        this.defaultCursor = defaultCursor;
        this.cursorClass = cursorClass;
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            for (int i = 0; i < syncThreadCount; i++) {
                int index = i;
                executorService.submit(() -> {
                    while (running.get()) {
                        try {
                            for (int j = 0; j < syncRingStripes; j++) {
                                if (threadIndex(j) == index) {
                                    syncStripe(j);
                                }
                            }
                            Thread.sleep(syncIntervalMillis);
                        } catch (InterruptedException e) {
                            LOG.info("Sync thread {} was interrupted", index);
                        } catch (Throwable t) {
                            LOG.error("Failure in sync thread {}", new Object[] { index }, t);
                            Thread.sleep(syncIntervalMillis);
                        }
                    }
                    return null;
                });
            }

            for (int i = 0; i < syncRingStripes; i++) {
                amzaClientAquariumProvider.register("sync-stripe-" + i);
            }
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            executorService.shutdownNow();
        }
    }

    public interface ProgressStream {
        boolean stream(MiruTenantId toTenantId, ProgressType type, int partitionId) throws Exception;
    }

    public void streamProgress(MiruTenantId fromTenantId, MiruTenantId toTenantId, ProgressStream stream) throws Exception {
        PartitionClient progressClient = progressClient();
        byte[] fromKey = progressKey(fromTenantId, toTenantId, null);
        byte[] toKey = WALKey.prefixUpperExclusive(fromKey);
        progressClient.scan(Consistency.leader_quorum, true,
            prefixedKeyRangeStream -> {
                return prefixedKeyRangeStream.stream(null, fromKey, null, toKey);
            },
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    MiruTenantId tenantId = progressToTenantId(key);
                    ProgressType type = progressType(key);
                    if (type != null) {
                        return stream.stream(tenantId, type, UIO.bytesInt(value));
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
        byte[] fromCursorKey = cursorKey(tenantId, null, null);
        byte[] toCursorKey = WALKey.prefixUpperExclusive(fromCursorKey);
        List<byte[]> cursorKeys = Lists.newArrayList();
        cursorClient.scan(Consistency.leader_quorum, false,
            prefixedKeyRangeStream -> prefixedKeyRangeStream.stream(null, fromCursorKey, null, toCursorKey),
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
        return amzaClientAquariumProvider.livelyEndState("sync-stripe-" + syncStripe);
    }

    private int threadIndex(int syncStripe) {
        return syncStripe % syncThreadCount;
    }

    private PartitionClient progressClient() throws Exception {
        return partitionClientProvider.getPartition(progressName(), 3, PROGRESS_PROPERTIES);
    }

    private PartitionName progressName() {
        byte[] name = ("sync-progress").getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, name, name);
    }

    private PartitionClient cursorClient() throws Exception {
        return partitionClientProvider.getPartition(cursorName(), 3, CURSOR_PROPERTIES);
    }

    private PartitionName cursorName() {
        byte[] name = ("sync-cursor").getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, name, name);
    }

    private void syncStripe(int stripe) throws Exception {
        if (!isElected(stripe)) {
            return;
        }

        LOG.info("Syncing stripe:{}", stripe);
        int tenantCount = 0;
        int activityCount = 0;
        Map<MiruTenantId, MiruSyncTenantConfig> tenantIds;
        if (whitelistConfigStorage != null) {
            tenantIds = whitelistConfigStorage.getAll();
        } else {
            tenantIds = Maps.newHashMap();
            List<MiruTenantId> allTenantIds = fromWALClient.getAllTenantIds();
            for (MiruTenantId tenantId : allTenantIds) {
                tenantIds.put(tenantId, new MiruSyncTenantConfig(
                    new String(tenantId.getBytes(), StandardCharsets.UTF_8),
                    new String(tenantId.getBytes(), StandardCharsets.UTF_8),
                    System.currentTimeMillis() - reverseSyncMaxAgeMillis,
                    Long.MAX_VALUE,
                    0,
                    MiruSyncTimeShiftStrategy.none
                ));
            }
        }
        for (Entry<MiruTenantId, MiruSyncTenantConfig> entry : tenantIds.entrySet()) {
            if (!isElected(stripe)) {
                break;
            }
            MiruTenantId fromTenantId = entry.getKey();
            MiruTenantId toTenantId = new MiruTenantId(entry.getValue().syncToTenantId.getBytes(StandardCharsets.UTF_8));
            int tenantStripe = Math.abs(fromTenantId.hashCode() % syncRingStripes);
            if (tenantStripe == stripe) {
                tenantCount++;
                ensureSchema(fromTenantId, toTenantId);
                if (!isElected(stripe)) {
                    break;
                }

                int synced = syncTenant(fromTenantId, entry.getValue(), stripe, forward);
                if (synced > 0) {
                    LOG.info("Synced stripe:{} tenantId:{} activities:{} type:{}", stripe, fromTenantId, synced, forward);
                }
                activityCount += synced;

                if (!isElected(stripe)) {
                    continue;
                }

                synced = syncTenant(fromTenantId, entry.getValue(), stripe, reverse);
                if (synced > 0) {
                    LOG.info("Synced stripe:{} tenantId:{} activities:{} type:{}", stripe, fromTenantId, synced, reverse);
                }
                activityCount += synced;
            }
        }
        LOG.info("Synced stripe:{} tenants:{} activities:{}", stripe, tenantCount, activityCount);
    }

    private void ensureSchema(MiruTenantId fromTenantId, MiruTenantId toTenantId) throws Exception {
        if (!registeredSchemas.containsEntry(fromTenantId, toTenantId)) {
            MiruSchema schema = schemaProvider.getSchema(fromTenantId);

            LOG.info("Submitting schema fromTenantId:{} toTenantId:{}", fromTenantId, toTenantId);
            toSyncClient.registerSchema(toTenantId, schema);
            registeredSchemas.put(fromTenantId, toTenantId);
        }
    }

    private int syncTenant(MiruTenantId fromTenantId, MiruSyncTenantConfig toTenantConfig, int stripe, ProgressType type) throws Exception {
        MiruTenantId toTenantId = new MiruTenantId(toTenantConfig.syncToTenantId.getBytes(StandardCharsets.UTF_8));
        TenantProgress progress = getTenantProgress(fromTenantId, toTenantId, stripe);
        if (!isElected(stripe)) {
            return 0;
        }
        if (type == reverse && progress.reversePartitionId == null) {
            // all done
            return 0;
        }

        MiruPartitionId partitionId = type == reverse ? progress.reversePartitionId : progress.forwardPartitionId;
        MiruActivityWALStatus status = fromWALClient.getActivityWALStatusForTenant(fromTenantId, partitionId);
        if (!isElected(stripe)) {
            return 0;
        }
        if (type == reverse) {
            long maxClockTimestamp = -1;
            for (WriterCount count : status.counts) {
                maxClockTimestamp = Math.max(maxClockTimestamp, count.clockTimestamp);
            }
            // TODO toTenantConfig.startTimestampMillis
            if (reverseSyncMaxAgeMillis != -1 && (maxClockTimestamp == -1 || maxClockTimestamp < System.currentTimeMillis() - reverseSyncMaxAgeMillis)) {
                if (maxAgeSet.add(new TenantAndPartition(fromTenantId, partitionId))) {
                    LOG.info("Reverse sync reached max age for tenant:{} partition:{} clock:{}", fromTenantId, partitionId, maxClockTimestamp);
                }
                return 0;
            } else if (status.ends.containsAll(status.begins)) {
                return syncTenantPartition(fromTenantId, toTenantId, stripe, partitionId, type, status);
            } else {
                LOG.error("Reverse sync encountered open tenant:{} partition:{}", fromTenantId, partitionId);
                return 0;
            }
        } else {
            return syncTenantPartition(fromTenantId, toTenantId, stripe, partitionId, type, status);
        }
    }

    private int syncTenantPartition(MiruTenantId fromTenantId,
        MiruTenantId toTenantId,
        int stripe,
        MiruPartitionId partitionId,
        ProgressType type,
        MiruActivityWALStatus status) throws Exception {

        C cursor = getTenantPartitionCursor(fromTenantId, toTenantId, partitionId);
        if (!isElected(stripe)) {
            return 0;
        }

        // ignore our own writerId when determining if a partition is closed
        List<Integer> begins = Lists.newArrayList(status.begins);
        List<Integer> ends = Lists.newArrayList(status.ends);
        begins.remove(new Integer(-1));
        ends.remove(new Integer(-1));

        boolean closed = ends.containsAll(begins);
        int numWriters = Math.max(begins.size(), 1);
        int lastIndex = -1;

        AtomicLong clockTimestamp = new AtomicLong(-1);
        MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory(clockTimestamp::get);

        int synced = 0;
        while (true) {
            long stopAtTimestamp = type == forward ? orderIdProvider.getApproximateId(System.currentTimeMillis() - forwardSyncDelayMillis) : -1;
            StreamBatch<MiruWALEntry, C> batch = fromWALClient.getActivity(fromTenantId, partitionId, cursor, batchSize, stopAtTimestamp);
            if (!isElected(stripe)) {
                return synced;
            }
            int activityTypes = 0;
            if (batch.activities != null && !batch.activities.isEmpty()) {
                List<MiruPartitionedActivity> activities = Lists.newArrayListWithCapacity(batch.activities.size());
                for (MiruWALEntry entry : batch.activities) {
                    if (entry.activity.type.isActivityType() && entry.activity.activity.isPresent()) {
                        activityTypes++;
                        // rough estimate of index depth
                        lastIndex = Math.max(lastIndex, numWriters * entry.activity.index);
                        // copy to new tenantId if necessary, and assign to fake writerId
                        MiruActivity fromActivity = entry.activity.activity.get();
                        MiruActivity toActivity = fromTenantId.equals(toTenantId) ? fromActivity : fromActivity.copyToTenantId(toTenantId);
                        // forge clock timestamp
                        clockTimestamp.set(entry.activity.clockTimestamp);
                        activities.add(partitionedActivityFactory.activity(-1,
                            partitionId,
                            lastIndex,
                            toActivity));
                    }
                }
                if (!activities.isEmpty()) {
                    // mimic writer by injecting begin activity with fake writerId
                    activities.add(partitionedActivityFactory.begin(-1, partitionId, toTenantId, lastIndex));
                    toSyncClient.writeActivity(toTenantId, partitionId, activities);
                    synced += activities.size();
                }
            }
            saveTenantPartitionCursor(fromTenantId, toTenantId, partitionId, batch.cursor);
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
            toSyncClient.writeActivity(toTenantId, partitionId, Collections.singletonList(
                partitionedActivityFactory.end(-1, partitionId, fromTenantId, lastIndex)));
            advanceTenantProgress(fromTenantId, toTenantId, partitionId, type);
        }
        return synced;
    }

    private void advanceTenantProgress(MiruTenantId fromTenantId, MiruTenantId toTenantId, MiruPartitionId partitionId, ProgressType type) throws Exception {
        MiruPartitionId advanced = (type == reverse) ? partitionId.prev() : partitionId.next();
        PartitionClient partitionClient = progressClient();
        byte[] progressKey = progressKey(fromTenantId, toTenantId, type);
        byte[] value = UIO.intBytes(advanced == null ? -1 : advanced.getId());
        partitionClient.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> commitKeyValueStream.commit(progressKey, value, -1, false),
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
        LOG.info("Tenant progress fromTenantId:{} toTenantId:{} type:{} advanced from:{} to:{}", fromTenantId, toTenantId, type, partitionId, advanced);
    }

    /**
     * @return null if finished, empty if not started, else the last synced partition
     */
    private TenantProgress getTenantProgress(MiruTenantId fromTenantId, MiruTenantId toTenantId, int stripe) throws Exception {
        int[] progressId = { Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE };
        streamProgress(fromTenantId, toTenantId, (toTenantId1, type, partitionId) -> {
            progressId[type.index] = partitionId;
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
                    commitKeyValueStream.commit(progressKey(fromTenantId, toTenantId, initial), UIO.intBytes(progressId[initial.index]), -1, false);
                    commitKeyValueStream.commit(progressKey(fromTenantId, toTenantId, reverse), UIO.intBytes(progressId[reverse.index]), -1, false);
                    commitKeyValueStream.commit(progressKey(fromTenantId, toTenantId, forward), UIO.intBytes(progressId[forward.index]), -1, false);
                    return true;
                },
                additionalSolverAfterNMillis,
                abandonSolutionAfterNMillis,
                Optional.empty());
            LOG.info("Initialized progress fromTenantId:{} toTenantId:{} initial:{} reverse:{} forward:{}",
                fromTenantId, toTenantId, progressId[initial.index], progressId[reverse.index], progressId[forward.index]);
        } else {
            LOG.info("Found progress fromTenantId:{} toTenantId:{} initial:{} reverse:{} forward:{}",
                fromTenantId, toTenantId, progressId[initial.index], progressId[reverse.index], progressId[forward.index]);
        }

        MiruPartitionId initialPartitionId = progressId[initial.index] == -1 ? null : MiruPartitionId.of(progressId[initial.index]);
        MiruPartitionId reversePartitionId = progressId[reverse.index] == -1 ? null : MiruPartitionId.of(progressId[reverse.index]);
        MiruPartitionId forwardPartitionId = progressId[forward.index] == -1 ? null : MiruPartitionId.of(progressId[forward.index]);

        return new TenantProgress(initialPartitionId, reversePartitionId, forwardPartitionId);
    }

    public C getTenantPartitionCursor(MiruTenantId fromTenantId, MiruTenantId toTenantId, MiruPartitionId partitionId) throws Exception {
        PartitionClient cursorClient = cursorClient();
        byte[] cursorKey = cursorKey(fromTenantId, toTenantId, partitionId);
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
        return result[0] != null ? (C) result[0] : defaultCursor;
    }

    private void saveTenantPartitionCursor(MiruTenantId fromTenantId, MiruTenantId toTenantId, MiruPartitionId partitionId, C cursor) throws Exception {
        PartitionClient cursorClient = cursorClient();
        byte[] cursorKey = cursorKey(fromTenantId, toTenantId, partitionId);
        byte[] value = mapper.writeValueAsBytes(cursor);
        cursorClient.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> commitKeyValueStream.commit(cursorKey, value, -1, false),
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
    }

    private MiruTenantId progressToTenantId(byte[] key) {
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

    private byte[] cursorKey(MiruTenantId fromTenantId, MiruTenantId toTenantId, MiruPartitionId partitionId) {
        if (toTenantId == null) {
            byte[] tenantBytes = fromTenantId.getBytes();
            byte[] key = new byte[2 + tenantBytes.length];
            UIO.unsignedShortBytes(tenantBytes.length, key, 0);
            UIO.writeBytes(tenantBytes, key, 2);
            return key;
        } else if (partitionId == null) {
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
            byte[] key = new byte[2 + fromTenantBytes.length + 2 + toTenantBytes.length + 4];
            UIO.unsignedShortBytes(fromTenantBytes.length, key, 0);
            UIO.writeBytes(fromTenantBytes, key, 2);
            UIO.unsignedShortBytes(toTenantBytes.length, key, 2 + fromTenantBytes.length);
            UIO.writeBytes(toTenantBytes, key, 2 + fromTenantBytes.length + 2);
            UIO.intBytes(partitionId.getId(), key, 2 + fromTenantBytes.length + 2 + toTenantBytes.length);
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
        private final MiruPartitionId forwardPartitionId;

        public TenantProgress(MiruPartitionId initialPartitionId,
            MiruPartitionId reversePartitionId,
            MiruPartitionId forwardPartitionId) {
            this.initialPartitionId = initialPartitionId;
            this.reversePartitionId = reversePartitionId;
            this.forwardPartitionId = forwardPartitionId;
        }
    }
}
