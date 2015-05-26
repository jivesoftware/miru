package com.jivesoftware.os.miru.writer.deployable.base;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.writer.deployable.MiruPartitioner;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Map;

/**
 * @author jonathan
 */
public class MiruActivityIngress {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruPartitioner miruPartitioner;
    private final Map<MiruTenantId, Boolean> latestAlignmentCache;

    public MiruActivityIngress(MiruPartitioner miruPartitioner,
        Map<MiruTenantId, Boolean> latestAlignmentCache) {
        this.miruPartitioner = miruPartitioner;
        this.latestAlignmentCache = latestAlignmentCache;
    }

    public void sendActivity(List<MiruActivity> activities, boolean recoverFromRemoval) throws Exception {
        ListMultimap<MiruTenantId, MiruActivity> activitiesPerTenant = ArrayListMultimap.create();
        for (MiruActivity activity : activities) {
            activitiesPerTenant.put(activity.tenantId, activity);
        }

        for (final MiruTenantId tenantId : activitiesPerTenant.keySet()) {
            checkForWriterAlignmentIfNecessary(tenantId);

            List<MiruActivity> tenantActivities = activitiesPerTenant.get(tenantId);
            miruPartitioner.writeActivities(tenantId, tenantActivities, recoverFromRemoval);
            LOG.inc("sendActivity>wal", tenantActivities.size());
            LOG.inc("sendActivity>wal", tenantActivities.size(), tenantId.toString());
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

    public void sendRead(MiruReadEvent readEvent) throws Exception {
        MiruTenantId tenantId = readEvent.tenantId;
        checkForWriterAlignmentIfNecessary(tenantId);
        miruPartitioner.writeReadEvent(tenantId, readEvent);
    }

    public void sendUnread(MiruReadEvent readEvent) throws Exception {
        MiruTenantId tenantId = readEvent.tenantId;
        checkForWriterAlignmentIfNecessary(tenantId);
        miruPartitioner.writeUnreadEvent(tenantId, readEvent);
    }

    public void sendAllRead(MiruReadEvent readEvent) throws Exception {
        MiruTenantId tenantId = readEvent.tenantId;
        checkForWriterAlignmentIfNecessary(tenantId);
        miruPartitioner.writeAllReadEvent(tenantId, readEvent);
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
