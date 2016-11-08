package com.jivesoftware.os.miru.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
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
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.sync.MiruSyncClient;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.StreamBatch;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final int syncRingStripes;
    private final ExecutorService executorService;
    private final int syncThreadCount;
    private final long syncIntervalMillis;
    private final MiruWALClient<C, S> fromWALClient;
    private final MiruSyncClient toSyncClient;
    private final PartitionClientProvider partitionClientProvider;
    private final ObjectMapper mapper;
    private final List<MiruTenantId> whitelist;
    private final int batchSize;
    private final long forwardSyncDelayMillis;
    private final C defaultCursor;
    private final Class<C> cursorClass;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final long additionalSolverAfterNMillis = 10_000; //TODO expose to conf?
    private final long abandonLeaderSolutionAfterNMillis = 30_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 60_000; //TODO expose to conf?

    public MiruSyncSender(AmzaClientAquariumProvider amzaClientAquariumProvider,
        int syncRingStripes,
        ExecutorService executorService,
        int syncThreadCount,
        long syncIntervalMillis,
        MiruWALClient<C, S> fromWALClient,
        MiruSyncClient toSyncClient,
        PartitionClientProvider partitionClientProvider,
        ObjectMapper mapper,
        List<MiruTenantId> whitelist,
        int batchSize,
        long forwardSyncDelayMillis,
        C defaultCursor,
        Class<C> cursorClass) {
        this.amzaClientAquariumProvider = amzaClientAquariumProvider;
        this.syncRingStripes = syncRingStripes;
        this.executorService = executorService;
        this.syncThreadCount = syncThreadCount;
        this.syncIntervalMillis = syncIntervalMillis;
        this.fromWALClient = fromWALClient;
        this.toSyncClient = toSyncClient;
        this.partitionClientProvider = partitionClientProvider;
        this.mapper = mapper;
        this.whitelist = whitelist;
        this.batchSize = batchSize;
        this.forwardSyncDelayMillis = forwardSyncDelayMillis;
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
                                    syncIndex(j);
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
        boolean stream(ProgressType type, int partitionId) throws Exception;
    }

    public void streamProgress(MiruTenantId tenantId, ProgressStream stream) throws Exception {
        PartitionClient progressClient = progressClient(tenantId);
        progressClient.get(Consistency.leader_quorum, null,
            unprefixedWALKeyStream -> {
                unprefixedWALKeyStream.stream(progressKey(tenantId, initial));
                unprefixedWALKeyStream.stream(progressKey(tenantId, reverse));
                unprefixedWALKeyStream.stream(progressKey(tenantId, forward));
                return true;
            },
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    ProgressType type = progressType(key);
                    if (type != null) {
                        return stream.stream(type, UIO.bytesInt(value));
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
        PartitionClient cursorClient = cursorClient(tenantId);
        byte[] fromKey = cursorKey(tenantId, null);
        byte[] toKey = WALKey.prefixUpperExclusive(fromKey);
        List<byte[]> cursorKeys = Lists.newArrayList();
        cursorClient.scan(Consistency.leader_quorum, false,
            prefixedKeyRangeStream -> prefixedKeyRangeStream.stream(null, fromKey, null, toKey),
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

        PartitionClient progressClient = progressClient(tenantId);
        progressClient.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> {
                commitKeyValueStream.commit(progressKey(tenantId, initial), null, -1, true);
                commitKeyValueStream.commit(progressKey(tenantId, reverse), null, -1, true);
                commitKeyValueStream.commit(progressKey(tenantId, forward), null, -1, true);
                return true;
            },
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());

        LOG.info("Reset progress for tenant:{} cursors:{}", tenantId, cursorKeys.size());
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

    private PartitionClient progressClient(MiruTenantId tenantId) throws Exception {
        return partitionClientProvider.getPartition(progressName(tenantId), 3, PROGRESS_PROPERTIES);
    }

    private PartitionName progressName(MiruTenantId tenantId) {
        byte[] name = ("sync-progress-" + tenantId).getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, name, name);
    }

    private PartitionClient cursorClient(MiruTenantId tenantId) throws Exception {
        return partitionClientProvider.getPartition(cursorName(tenantId), 3, CURSOR_PROPERTIES);
    }

    private PartitionName cursorName(MiruTenantId tenantId) {
        byte[] name = ("sync-cursor-" + tenantId).getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, name, name);
    }

    private void syncIndex(int stripe) throws Exception {
        if (!isElected(stripe)) {
            return;
        }

        LOG.info("Syncing stripe:{}", stripe);
        int tenantCount = 0;
        int activityCount = 0;
        List<MiruTenantId> tenantIds = whitelist != null ? whitelist : fromWALClient.getAllTenantIds();
        for (MiruTenantId tenantId : tenantIds) {
            if (!isElected(stripe)) {
                break;
            }
            int tenantStripe = Math.abs(tenantId.hashCode() % syncRingStripes);
            if (tenantStripe == stripe) {
                tenantCount++;

                int synced = syncTenant(tenantId, stripe, forward);
                if (synced > 0) {
                    LOG.info("Synced tenantId:{} activities:{} type:{}", tenantId, synced, reverse);
                }
                activityCount += synced;

                if (!isElected(stripe)) {
                    continue;
                }

                synced = syncTenant(tenantId, stripe, reverse);
                if (synced > 0) {
                    LOG.info("Synced tenantId:{} activities:{} type:{}", tenantId, synced, reverse);
                }
                activityCount += synced;
            }
        }
        LOG.info("Synced stripe:{} tenants:{} activities:{}", stripe, tenantCount, activityCount);
    }

    private int syncTenant(MiruTenantId tenantId, int stripe, ProgressType type) throws Exception {
        TenantProgress progress = getTenantProgress(tenantId, stripe);
        if (!isElected(stripe)) {
            return 0;
        }
        if (type == reverse && progress.reversePartitionId == null) {
            // all done
            return 0;
        }

        MiruPartitionId partitionId = type == reverse ? progress.reversePartitionId : progress.forwardPartitionId;
        MiruActivityWALStatus status = fromWALClient.getActivityWALStatusForTenant(tenantId, partitionId);
        if (!isElected(stripe)) {
            return 0;
        }
        if (type == reverse) {
            if (status.ends.containsAll(status.begins)) {
                return syncTenantPartition(tenantId, stripe, partitionId, type);
            } else {
                LOG.error("Reverse sync encountered open tenant:{} partition:{}", tenantId, partitionId);
                return 0;
            }
        } else {
            return syncTenantPartition(tenantId, stripe, partitionId, type);
        }
    }

    private int syncTenantPartition(MiruTenantId tenantId, int stripe, MiruPartitionId partitionId, ProgressType type) throws Exception {
        C cursor = getTenantPartitionCursor(tenantId, partitionId);
        if (!isElected(stripe)) {
            return 0;
        }

        MiruActivityWALStatus status = fromWALClient.getActivityWALStatusForTenant(tenantId, partitionId);
        boolean closed = status.ends.containsAll(status.begins);

        int synced = 0;
        while (true) {
            StreamBatch<MiruWALEntry, C> batch = fromWALClient.getActivity(tenantId, partitionId, cursor, batchSize);
            if (!isElected(stripe)) {
                return synced;
            }
            int activityTypes = 0;
            if (batch.activities != null && !batch.activities.isEmpty()) {
                List<MiruPartitionedActivity> activities = Lists.newArrayListWithCapacity(batch.activities.size());
                long pauseOnClockTimestamp = type == forward ? System.currentTimeMillis() + forwardSyncDelayMillis : Long.MAX_VALUE;
                for (MiruWALEntry activity : batch.activities) {
                    if (activity.activity.type.isActivityType()) {
                        activityTypes++;
                    }
                    if (activity.activity.clockTimestamp > pauseOnClockTimestamp) {
                        LOG.warn("Paused sync for tenant:{} partition:{} for clock:{} age:{}",
                            tenantId, partitionId, activity.activity.clockTimestamp, System.currentTimeMillis() - activity.activity.clockTimestamp);
                        break;
                    } else {
                        activities.add(activity.activity);
                    }
                }
                toSyncClient.writeActivity(tenantId, partitionId, activities);
                synced += activities.size();
            }
            saveTenantPartitionCursor(tenantId, partitionId, batch.cursor);
            cursor = batch.cursor;
            if (activityTypes == 0) {
                break;
            }
        }

        // if it closed while syncing, we'll catch it next time
        if (closed) {
            advanceTenantProgress(tenantId, partitionId, type);
        }
        return synced;
    }

    private void advanceTenantProgress(MiruTenantId tenantId, MiruPartitionId partitionId, ProgressType type) throws Exception {
        MiruPartitionId advanced = (type == reverse) ? partitionId.prev() : partitionId.next();
        PartitionClient partitionClient = progressClient(tenantId);
        byte[] progressKey = progressKey(tenantId, type);
        byte[] value = UIO.intBytes(advanced == null ? -1 : advanced.getId());
        partitionClient.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> commitKeyValueStream.commit(progressKey, value, -1, false),
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
        LOG.info("Tenant progress for tenant:{} type:{} advanced from:{} to:{}", tenantId, type, partitionId, advanced);
    }

    /**
     * @return null if finished, empty if not started, else the last synced partition
     */
    private TenantProgress getTenantProgress(MiruTenantId tenantId, int stripe) throws Exception {
        int[] progressId = { Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE };
        streamProgress(tenantId, (type, partitionId) -> {
            progressId[type.index] = partitionId;
            return true;
        });

        if (progressId[initial.index] == Integer.MIN_VALUE) {
            MiruPartitionId largestPartitionId = fromWALClient.getLargestPartitionId(tenantId);
            MiruPartitionId prevPartitionId = largestPartitionId == null ? null : largestPartitionId.prev();
            progressId[initial.index] = largestPartitionId == null ? 0 : largestPartitionId.getId();
            progressId[reverse.index] = prevPartitionId == null ? -1 : prevPartitionId.getId();
            progressId[forward.index] = largestPartitionId == null ? 0 : largestPartitionId.getId();

            if (!isElected(stripe)) {
                throw new IllegalStateException("Lost leadership while initializing progress for tenant:" + tenantId + " stripe:" + stripe);
            }

            PartitionClient progressClient = progressClient(tenantId);
            progressClient.commit(Consistency.leader_quorum, null,
                commitKeyValueStream -> {
                    commitKeyValueStream.commit(progressKey(tenantId, initial), UIO.intBytes(progressId[initial.index]), -1, false);
                    commitKeyValueStream.commit(progressKey(tenantId, reverse), UIO.intBytes(progressId[reverse.index]), -1, false);
                    commitKeyValueStream.commit(progressKey(tenantId, forward), UIO.intBytes(progressId[forward.index]), -1, false);
                    return true;
                },
                additionalSolverAfterNMillis,
                abandonSolutionAfterNMillis,
                Optional.empty());
            LOG.info("Initialized progress for tenant:{} initial:{} reverse:{} forward:{}",
                tenantId, progressId[initial.index], progressId[reverse.index], progressId[forward.index]);
        } else {
            LOG.info("Found progress for tenant:{} initial:{} reverse:{} forward:{}",
                tenantId, progressId[initial.index], progressId[reverse.index], progressId[forward.index]);
        }

        MiruPartitionId initialPartitionId = progressId[initial.index] == -1 ? null : MiruPartitionId.of(progressId[initial.index]);
        MiruPartitionId reversePartitionId = progressId[reverse.index] == -1 ? null : MiruPartitionId.of(progressId[reverse.index]);
        MiruPartitionId forwardPartitionId = progressId[forward.index] == -1 ? null : MiruPartitionId.of(progressId[forward.index]);

        return new TenantProgress(initialPartitionId, reversePartitionId, forwardPartitionId);
    }

    public C getTenantPartitionCursor(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        PartitionClient cursorClient = cursorClient(tenantId);
        byte[] cursorKey = cursorKey(tenantId, partitionId);
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

    private void saveTenantPartitionCursor(MiruTenantId tenantId, MiruPartitionId partitionId, C cursor) throws Exception {
        PartitionClient cursorClient = cursorClient(tenantId);
        byte[] cursorKey = cursorKey(tenantId, partitionId);
        byte[] value = mapper.writeValueAsBytes(cursor);
        cursorClient.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> commitKeyValueStream.commit(cursorKey, value, -1, false),
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
    }

    private ProgressType progressType(byte[] key) {
        return ProgressType.fromIndex(key[key.length - 1]);
    }

    private byte[] progressKey(MiruTenantId tenantId, ProgressType type) {
        byte[] tenantBytes = tenantId.getBytes();
        byte[] key = new byte[2 + tenantBytes.length + 1];
        UIO.unsignedShortBytes(tenantBytes.length, key, 0);
        UIO.writeBytes(tenantBytes, key, 2);
        key[2 + tenantBytes.length] = type.index;
        return key;
    }

    private byte[] cursorKey(MiruTenantId tenantId, MiruPartitionId partitionId) {
        if (partitionId == null) {
            byte[] tenantBytes = tenantId.getBytes();
            byte[] key = new byte[2 + tenantBytes.length];
            UIO.unsignedShortBytes(tenantBytes.length, key, 0);
            UIO.writeBytes(tenantBytes, key, 2);
            return key;
        } else {
            byte[] tenantBytes = tenantId.getBytes();
            byte[] key = new byte[2 + tenantBytes.length + 4];
            UIO.unsignedShortBytes(tenantBytes.length, key, 0);
            UIO.writeBytes(tenantBytes, key, 2);
            UIO.intBytes(partitionId.getId(), key, 2 + tenantBytes.length);
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
