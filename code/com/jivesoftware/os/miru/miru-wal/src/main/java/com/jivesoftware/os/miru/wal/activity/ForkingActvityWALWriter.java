/*
 * Copyright 2015 jonathan.colt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.miru.wal.activity;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class ForkingActvityWALWriter implements MiruActivityWALWriter {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruActivityWALWriter primaryWAL;
    private final MiruActivityWALWriter secondaryWAL;

    public ForkingActvityWALWriter(MiruActivityWALWriter primaryWAL, MiruActivityWALWriter secondaryWAL) {
        this.primaryWAL = primaryWAL;
        this.secondaryWAL = secondaryWAL;
    }

    @Override
    public void write(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        if (secondaryWAL != null) {
            LOG.startTimer("forking>seconday");
            try {
                while (true) {
                    try {
                        secondaryWAL.write(tenantId, partitionedActivities);
                        break;
                    } catch (Exception x) {
                        LOG.warn("Failed to write:{} activities for tenant:{} to seconday WAL.", new Object[]{partitionedActivities.size(), tenantId}, x);
                        Thread.sleep(10_000);
                    }
                }
            } finally {
                LOG.stopTimer("forking>seconday");
            }

            LOG.startTimer("forking>primary");
            try {
                primaryWAL.write(tenantId, partitionedActivities);
            } finally {
                LOG.startTimer("forking>primary");
            }
        }
    }

}
