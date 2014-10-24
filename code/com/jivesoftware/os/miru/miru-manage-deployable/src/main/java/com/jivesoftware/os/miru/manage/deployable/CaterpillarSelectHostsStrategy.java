package com.jivesoftware.os.miru.manage.deployable;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class CaterpillarSelectHostsStrategy implements SelectHostsStrategy {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final int numberOfPlaces;
    private final boolean currentPartitionOnly;

    public CaterpillarSelectHostsStrategy(int numberOfPlaces, boolean currentPartitionOnly) {
        this.numberOfPlaces = numberOfPlaces;
        this.currentPartitionOnly = currentPartitionOnly;
    }

    @Override
    public boolean isCurrentPartitionOnly() {
        return currentPartitionOnly;
    }

    @Override
    public List<MiruHost> selectHosts(MiruHost fromHost, List<MiruHost> allHosts, List<MiruPartition> partitions, int numberOfReplicas) {
        int numHosts = allHosts.size();
        List<MiruHost> hostsToElect = Lists.newArrayListWithCapacity(numberOfReplicas);
        int fromIndex = allHosts.indexOf(fromHost);
        if (partitions.isEmpty()) {
            for (int index = fromIndex, j = 0; j < numberOfReplicas && j < numHosts; index++, j++) {
                MiruHost hostToElect = allHosts.get(index % numHosts);
                LOG.debug("Empty {}", hostToElect);
                hostsToElect.add(hostToElect);
            }
        } else {
            Set<MiruHost> hostsWithReplica = Sets.newHashSet();
            List<Integer> hostIndexes = Lists.newArrayListWithCapacity(hostsWithReplica.size());
            for (MiruPartition partition : partitions) {
                hostsWithReplica.add(partition.coord.host);
                hostIndexes.add(allHosts.indexOf(partition.coord.host));
            }
            Collections.sort(hostIndexes);
            int start = startOfContiguousRun(numHosts, hostIndexes);

            boolean contiguous = (start >= 0);
            if (contiguous) {
                if (numberOfPlaces != 0) {
                    //    \_/-.--.--.--.--.--.
                    //    (")__)__)__)__)__)__)
                    //     ^ "" "" "" "" "" ""
                    // magical caterpillar to the right, since walking from next neighbor might cycle
                    // e.g. hosts=0-9, partitions=0,8,9, shift(0)=8,9,0
                    // instead we do shift(0)=9,0,1
                    for (int index = hostIndexes.get(start) + numHosts + numberOfPlaces, j = 0; j < numberOfReplicas && j < numHosts; index++, j++) {
                        MiruHost hostToElect = allHosts.get(index % numHosts);
                        LOG.debug("Caterpillar {}", hostToElect);
                        hostsToElect.add(hostToElect);
                    }
                } else {
                    if (fromIndex == hostIndexes.get(start)) {
                        // evac right
                        for (int index = fromIndex + 1, j = 0; j < numberOfReplicas && j < numHosts; index++, j++) {
                            MiruHost hostToElect = allHosts.get(index % numHosts);
                            LOG.debug("EvacRight {}", hostToElect);
                            hostsToElect.add(hostToElect);
                        }
                    } else {
                        // evac left
                        for (int index = fromIndex + numHosts - numberOfReplicas, j = 0; j < numberOfReplicas && j < numHosts; index++, j++) {
                            MiruHost hostToElect = allHosts.get(index % numHosts);
                            LOG.debug("EvacLeft {}", hostToElect);
                            hostsToElect.add(hostToElect);
                        }
                    }
                }
            } else {
                // safe to walk from next neighbor
                int neighborIndex = -1;
                for (int index = fromIndex + 1, j = 0; j < numHosts; index++, j++) {
                    if (hostIndexes.contains(index % numHosts)) {
                        neighborIndex = index;
                        break;
                    }
                }
                //TODO consider adding isOnline check
                for (int index = neighborIndex, j = 0; j < numberOfReplicas && j < numHosts; index++, j++) {
                    MiruHost hostToElect = allHosts.get(index % numHosts);
                    LOG.debug("Neighbor {}", hostToElect);
                    hostsToElect.add(hostToElect);
                }
            }
        }
        return hostsToElect;
    }

    // example:
    //   0. 1. 2.
    // [ 0, 8, 9 ]
    protected int startOfContiguousRun(int numHosts, List<Integer> hostIndexes) {
        // i=0. index=0, contains(9)? -> yes
        // i=1. index=8, contains(7)? -> no. start=1
        int start = -1;
        for (int i = 0; i < hostIndexes.size(); i++) {
            int index = hostIndexes.get(i);
            if (!hostIndexes.contains((index + numHosts - 1) % numHosts)) {
                start = i;
                break;
            }
        }

        // start=1
        // index=8, j=0<3. contains(8)? -> yes
        // index=9, j=1<3. contains(9)? -> yes
        // index=10, j=2<3. contains(0)? -> yes
        if (start >= 0) {
            for (int index = hostIndexes.get(start), j = 0; j < hostIndexes.size(); index++, j++) {
                if (!hostIndexes.contains(index % numHosts)) {
                    start = -1;
                    break;
                }
            }
        }
        return start;
    }
}
