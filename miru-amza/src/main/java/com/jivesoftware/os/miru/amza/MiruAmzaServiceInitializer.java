package com.jivesoftware.os.miru.amza;

import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.scan.RowChanges;
import com.jivesoftware.os.amza.embed.EmbedAmzaServiceInitializer;
import com.jivesoftware.os.amza.embed.EmbedAmzaServiceInitializer.Lifecycle;
import com.jivesoftware.os.amza.lab.pointers.LABPointerIndexConfig;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.http.client.ClientHealthProvider;
import java.util.Set;

/**
 *
 */
public class MiruAmzaServiceInitializer {

    public AmzaService initialize(Deployable deployable,
        ClientHealthProvider clientHealthProvider,
        int instanceId,
        String instanceKey,
        String serviceName,
        String datacenterName,
        String rackName,
        String hostName,
        int port,
        boolean authEnabled,
        String clusterName,
        MiruAmzaServiceConfig config,
        boolean useAmzaDiscovery,
        RowChanges allRowChanges) throws Exception {

        SnowflakeIdPacker idPacker = new SnowflakeIdPacker();
        JiveEpochTimestampProvider timestampProvider = new JiveEpochTimestampProvider();

        AmzaStats amzaStats = new AmzaStats();
        BAInterner baInterner = new BAInterner();

        AmzaServiceConfig amzaServiceConfig = new AmzaServiceConfig();
        amzaServiceConfig.workingDirectories = config.getWorkingDirectories().split(",");
        amzaServiceConfig.checkIfCompactionIsNeededIntervalInMillis = config.getCheckIfCompactionIsNeededIntervalInMillis();
        amzaServiceConfig.deltaStripeCompactionIntervalInMillis = config.getDeltaStripeCompactionIntervalInMillis();
        amzaServiceConfig.deltaMaxValueSizeInIndex = config.getDeltaMaxValueSizeInIndex();
        amzaServiceConfig.corruptionParanoiaFactor = config.getCorruptionParanoiaFactor();
        amzaServiceConfig.maxUpdatesBeforeDeltaStripeCompaction = config.getMaxUpdatesBeforeDeltaStripeCompaction();
        amzaServiceConfig.numberOfTakerThreads = config.getNumberOfTakerThreads();
        amzaServiceConfig.hardFsync = config.getHardFsync();
        amzaServiceConfig.takeSlowThresholdInMillis = config.getTakeSlowThresholdInMillis();
        amzaServiceConfig.rackDistributionEnabled = config.getRackDistributionEnabled();
        amzaServiceConfig.ackWatersVerboseLogTimeouts = config.getAckWatersVerboseLogTimeouts();

        LABPointerIndexConfig amzaLabConfig = deployable.config(LABPointerIndexConfig.class);

        String blacklist = config.getBlacklistRingMembers();
        Set<RingMember> blacklistRingMembers = Sets.newHashSet();
        for (String b : blacklist != null ? blacklist.split("\\s*,\\s*") : new String[0]) {
            if (b != null) {
                b = b.trim();
                if (!b.isEmpty()) {
                    blacklistRingMembers.add(new RingMember(b));
                }
            }
        }

        Lifecycle lifecycle = new EmbedAmzaServiceInitializer().initialize(deployable,
            clientHealthProvider,
            instanceId,
            instanceKey,
            serviceName,
            datacenterName,
            rackName,
            hostName,
            port,
            authEnabled,
            clusterName,
            amzaServiceConfig,
            amzaLabConfig,
            amzaStats,
            baInterner,
            idPacker,
            timestampProvider,
            blacklistRingMembers,
            true,
            false,
            allRowChanges);
        AmzaService amzaService = lifecycle.amzaService;

        lifecycle.startAmzaService();

        if (useAmzaDiscovery) {
            lifecycle.startRoutingBirdAmzaDiscovery();
        }

        return amzaService;
    }
}
