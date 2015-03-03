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
package com.jivesoftware.os.miru.plugin.partition;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;

/**
 *
 * @author jonathan.colt
 */
public class MiruRoutablePartition {

    public final MiruHost host;
    public final MiruPartitionId partitionId;
    public final boolean local;
    public final MiruPartitionState state;
    public final MiruBackingStorage storage;

    public MiruRoutablePartition(MiruHost host,
        MiruPartitionId partitionId,
        boolean local,
        MiruPartitionState state,
        MiruBackingStorage storage) {
        this.host = host;
        this.partitionId = partitionId;
        this.local = local;
        this.state = state;
        this.storage = storage;
    }

}
