package com.jivesoftware.os.miru.writer.deployable.base;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.api.activity.MiruStreamEvent;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.writer.deployable.MiruPartitioner;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.jetbrains.annotations.NotNull;

/**
 * @author jonathan
 */
public class MiruActivityIngress {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruPartitioner miruPartitioner;
    private final Map<MiruTenantId, Boolean> latestAlignmentCache;
    private final ExecutorService sendActivitiesExecutorService;

    public MiruActivityIngress(MiruPartitioner miruPartitioner,
        Map<MiruTenantId, Boolean> latestAlignmentCache,
        ExecutorService sendActivitiesExecutorService) {
        this.miruPartitioner = miruPartitioner;
        this.latestAlignmentCache = latestAlignmentCache;
        this.sendActivitiesExecutorService = sendActivitiesExecutorService;
    }

    public void sendActivity(List<MiruActivity> activities, boolean recoverFromRemoval) throws Exception {
        ListMultimap<MiruTenantId, MiruActivity> activitiesPerTenant = ArrayListMultimap.create();
        for (MiruActivity activity : activities) {
            activitiesPerTenant.put(activity.tenantId, activity);
        }

        List<Future<?>> futures = Lists.newArrayList();
        for (final MiruTenantId tenantId : activitiesPerTenant.keySet()) {
            futures.add(sendActivitiesExecutorService.submit(() -> {
                checkForWriterAlignmentIfNecessary(tenantId);

                List<MiruActivity> tenantActivities = activitiesPerTenant.get(tenantId);
                miruPartitioner.writeActivities(tenantId, tenantActivities, recoverFromRemoval);
                LOG.inc("sendActivity>wal", tenantActivities.size());
                LOG.inc("sendActivity>wal", tenantActivities.size(), tenantId.toString());
                return null;
            }));
        }
        for (Future<?> future : futures) {
            future.get();
        }
    }

    public void removeActivity(List<MiruActivity> activities) throws Exception {
        ListMultimap<MiruTenantId, MiruActivity> activitiesPerTenant = ArrayListMultimap.create();
        for (MiruActivity activity : activities) {
            activitiesPerTenant.put(activity.tenantId, activity);
        }

        for (final MiruTenantId tenantId : activitiesPerTenant.keySet()) {
            checkForWriterAlignmentIfNecessary(tenantId);

            List<MiruActivity> tenantActivities = activitiesPerTenant.get(tenantId);
            miruPartitioner.removeActivities(tenantId, tenantActivities);
        }
    }

    public void sendStreamEvents(List<MiruStreamEvent> streamEvents) {
        ListMultimap<MiruTenantId, MiruStreamEvent> tenantStreamEvents = ArrayListMultimap.create();
        for (MiruStreamEvent streamEvent : streamEvents) {
            tenantStreamEvents.put(streamEvent.tenantId, streamEvent);
        }
        for (MiruTenantId tenantId : tenantStreamEvents.keySet()) {
            checkForWriterAlignmentIfNecessary(tenantId);
            List<MiruStreamEvent> events = tenantStreamEvents.get(tenantId);
            //TODO miruPartitioner.writeStreamEvents(tenantId, events);
        }
    }

    public void sendRead(List<MiruReadEvent> readEvents) throws Exception {
        ListMultimap<MiruTenantId, MiruReadEvent> tenantReadEvents = splitByTenant(readEvents);
        for (MiruTenantId tenantId : tenantReadEvents.keySet()) {
            checkForWriterAlignmentIfNecessary(tenantId);
            List<MiruReadEvent> events = tenantReadEvents.get(tenantId);
            miruPartitioner.writeReadEvents(tenantId, events);
        }
    }

    public void sendUnread(List<MiruReadEvent> readEvents) throws Exception {
        ListMultimap<MiruTenantId, MiruReadEvent> tenantReadEvents = splitByTenant(readEvents);
        for (MiruTenantId tenantId : tenantReadEvents.keySet()) {
            checkForWriterAlignmentIfNecessary(tenantId);
            List<MiruReadEvent> events = tenantReadEvents.get(tenantId);
            miruPartitioner.writeUnreadEvents(tenantId, events);
        }
    }

    public void sendAllRead(List<MiruReadEvent> readEvents) throws Exception {
        ListMultimap<MiruTenantId, MiruReadEvent> tenantReadEvents = splitByTenant(readEvents);
        for (MiruTenantId tenantId : tenantReadEvents.keySet()) {
            checkForWriterAlignmentIfNecessary(tenantId);
            List<MiruReadEvent> events = tenantReadEvents.get(tenantId);
            miruPartitioner.writeAllReadEvents(tenantId, events);
        }
    }

    private ListMultimap<MiruTenantId, MiruReadEvent> splitByTenant(List<MiruReadEvent> readEvents) {
        ListMultimap<MiruTenantId, MiruReadEvent> tenantReadEvents = ArrayListMultimap.create();
        for (MiruReadEvent readEvent : readEvents) {
            tenantReadEvents.put(readEvent.tenantId, readEvent);
        }
        return tenantReadEvents;
    }

    public void updateCursor(int writerId, MiruTenantId tenantId, MiruPartitionId partitionId, int index) throws Exception {
        miruPartitioner.updateCursor(writerId, tenantId, partitionId, index);
    }

    private void checkForWriterAlignmentIfNecessary(MiruTenantId tenantId) {
        // the cache limits how often we check for alignment per tenant
        if (!latestAlignmentCache.containsKey(tenantId)) {
            try {
                latestAlignmentCache.put(tenantId, true);
                miruPartitioner.checkForAlignmentWithOtherWriters(tenantId);
                LOG.inc("alignWriters>aligned", tenantId.toString());
            } catch (Throwable t) {
                LOG.error("Unable to check for alignment with other writers", t);
                LOG.inc("alignWriters>failed", tenantId.toString());
            }
        } else {
            LOG.inc("alignWriters>skipped", tenantId.toString());
        }
    }
}
