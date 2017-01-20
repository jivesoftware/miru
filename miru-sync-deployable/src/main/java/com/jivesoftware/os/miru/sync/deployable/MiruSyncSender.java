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
    private final int syncRingStripes;
    private final ScheduledExecutorService executorService;
    private final ScheduledFuture[] syncFutures;
    private final MiruSchemaProvider schemaProvider;
    private final MiruWALClient<C, S> fromWALClient;
    private final MiruSyncClient toSyncClient;
    private final PartitionClientProvider partitionClientProvider;
    private final ObjectMapper mapper;
    private final MiruSyncConfigProvider syncConfigProvider;
    private final C defaultCursor;
    private final Class<C> cursorClass;

    private final Set<TenantTuplePartition> maxAgeSet = Collections.newSetFromMap(Maps.newConcurrentMap());
    private final SetMultimap<MiruTenantId, MiruTenantId> registeredSchemas = Multimaps.synchronizedSetMultimap(HashMultimap.create());
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final long additionalSolverAfterNMillis = 10_000; //TODO expose to conf?
    private final long abandonLeaderSolutionAfterNMillis = 30_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 60_000; //TODO expose to conf?

    public MiruSyncSender(MiruStats stats,
        MiruSyncSenderConfig config,
        AmzaClientAquariumProvider amzaClientAquariumProvider,
        TimestampedOrderIdProvider orderIdProvider,
        int syncRingStripes,
        ScheduledExecutorService executorService,
        MiruSchemaProvider schemaProvider,
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
        this.syncRingStripes = syncRingStripes;
        this.executorService = executorService;
        this.syncFutures = new ScheduledFuture[syncRingStripes];
        this.schemaProvider = schemaProvider;
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

    public void start() {
        if (running.compareAndSet(false, true)) {
            for (int i = 0; i < syncRingStripes; i++) {
                amzaClientAquariumProvider.register(aquariumName(i));
            }

            for (int i = 0; i < syncRingStripes; i++) {
                int index = i;
                syncFutures[i] = executorService.scheduleWithFixedDelay(() -> {
                    try {
                        syncStripe(index);
                    } catch (InterruptedException e) {
                        LOG.info("Sync thread {} was interrupted", index);
                    } catch (Throwable t) {
                        LOG.error("Failure in sync thread {}", new Object[] { index }, t);
                    }
                }, 0, config.syncIntervalMillis, TimeUnit.MILLISECONDS);
            }
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            for (int i = 0; i < syncRingStripes; i++) {
                syncFutures[i].cancel(true);
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

    private void syncStripe(int stripe) throws Exception {
        if (!isElected(stripe)) {
            return;
        }

        LOG.info("Syncing stripe:{}", stripe);
        int tenantCount = 0;
        int activityCount = 0;
        Map<MiruSyncTenantTuple, MiruSyncTenantConfig> tenantIds;
        tenantIds = syncConfigProvider.getAll(config.name);

        for (Entry<MiruSyncTenantTuple, MiruSyncTenantConfig> entry : tenantIds.entrySet()) {
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

                int synced = syncTenant(tenantTuple, entry.getValue(), stripe, forward);
                if (synced > 0) {
                    LOG.info("Synced stripe:{} tenantId:{} activities:{} type:{}", stripe, tenantTuple.from, synced, forward);
                }
                activityCount += synced;

                if (!isElected(stripe)) {
                    continue;
                }

                synced = syncTenant(tenantTuple, entry.getValue(), stripe, reverse);
                if (synced > 0) {
                    LOG.info("Synced stripe:{} tenantId:{} activities:{} type:{}", stripe, tenantTuple.from, synced, reverse);
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

    private int syncTenant(MiruSyncTenantTuple tenantTuple, MiruSyncTenantConfig tenantConfig, int stripe, ProgressType type) throws Exception {
        TenantProgress progress = getTenantProgress(tenantTuple.from, tenantTuple.to, stripe);
        if (!isElected(stripe)) {
            return 0;
        }
        if (type == reverse && progress.reversePartitionId == null) {
            // all done
            return 0;
        }

        MiruPartitionId partitionId = type == reverse ? progress.reversePartitionId : progress.forwardPartitionId;
        MiruActivityWALStatus status = fromWALClient.getActivityWALStatusForTenant(tenantTuple.from, partitionId);
        if (!isElected(stripe)) {
            return 0;
        }
        if (type == reverse) {
            long maxClockTimestamp = -1;
            for (WriterCount count : status.counts) {
                maxClockTimestamp = Math.max(maxClockTimestamp, count.clockTimestamp);
            }
            if (tenantConfig.startTimestampMillis != -1 && (maxClockTimestamp == -1 || maxClockTimestamp < tenantConfig.startTimestampMillis)) {
                TenantTuplePartition tenantTuplePartition = new TenantTuplePartition(tenantTuple, partitionId);
                if (!maxAgeSet.contains(tenantTuplePartition)) {
                    LOG.info("Reverse sync reached max age from:{} to:{} partition:{} clock:{}",
                        tenantTuple.from, tenantTuple.to, partitionId, maxClockTimestamp);
                    setTenantProgress(tenantTuple, partitionId, type, false);
                    maxAgeSet.add(tenantTuplePartition);
                }
                return 0;
            } else if (status.ends.containsAll(status.begins)) {
                return syncTenantPartition(tenantTuple, stripe, partitionId, type, status);
            } else {
                LOG.error("Reverse sync encountered open partition from:{} to:{} partition:{}", tenantTuple.from, tenantTuple.to, partitionId);
                return 0;
            }
        } else {
            return syncTenantPartition(tenantTuple, stripe, partitionId, type, status);
        }
    }

    private int syncTenantPartition(MiruSyncTenantTuple tenantTuple,
        int stripe,
        MiruPartitionId partitionId,
        ProgressType type,
        MiruActivityWALStatus status) throws Exception {

        C cursor = getTenantPartitionCursor(tenantTuple.from, tenantTuple.to, partitionId);
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

        String readableFromTo = tenantTuple.from.toString() + "/" + tenantTuple.to.toString();
        String statsBytes = "sender/sync/" + readableFromTo + "/bytes";
        String statsCount = "sender/sync/" + readableFromTo + "/count";

        boolean took = false;
        int synced = 0;
        while (true) {
            MutableLong bytesCount = new MutableLong();
            long stopAtTimestamp = type == forward ? orderIdProvider.getApproximateId(System.currentTimeMillis() - config.forwardSyncDelayMillis) : -1;
            long start = System.currentTimeMillis();
            StreamBatch<MiruWALEntry, C> batch = fromWALClient.getActivity(tenantTuple.from, partitionId, cursor, config.batchSize, stopAtTimestamp,
                bytesCount);
            if (!isElected(stripe)) {
                return synced;
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
                        // rough estimate of index depth
                        lastIndex = Math.max(lastIndex, numWriters * entry.activity.index);
                        // copy to new tenantId if necessary, and assign to fake writerId
                        MiruActivity fromActivity = entry.activity.activity.get();
                        MiruActivity toActivity = tenantTuple.from.equals(tenantTuple.to) ? fromActivity : fromActivity.copyToTenantId(tenantTuple.to);
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
            saveTenantPartitionCursor(tenantTuple, partitionId, batch.cursor);
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
                partitionedActivityFactory.end(-1, partitionId, tenantTuple.from, lastIndex)));
            advanceTenantProgress(tenantTuple, partitionId, type);
        } else if (took) {
            // took at least one thing and caught up, so set 'taking' back to false
            setTenantProgress(tenantTuple, partitionId, type, false);
        }
        return synced;
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

    private void saveTenantPartitionCursor(MiruSyncTenantTuple tenantTuple, MiruPartitionId partitionId, C cursor) throws Exception {
        PartitionClient cursorClient = cursorClient();
        byte[] cursorKey = cursorKey(tenantTuple.from, tenantTuple.to, partitionId);
        byte[] value = mapper.writeValueAsBytes(cursor);
        cursorClient.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> commitKeyValueStream.commit(cursorKey, value, -1, false),
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
        forward((byte) 2),
        status((byte) 3);

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
