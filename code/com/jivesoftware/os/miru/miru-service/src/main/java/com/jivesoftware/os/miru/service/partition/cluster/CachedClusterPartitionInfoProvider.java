/*
 * Copyright 2014 jivesoftwar.
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
package com.jivesoftware.os.miru.service.partition.cluster;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.service.partition.MiruPartitionInfoProvider;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

/**
 *
 * @author jonathan
 */
public class CachedClusterPartitionInfoProvider implements MiruPartitionInfoProvider {

    private final ConcurrentMap<MiruPartitionCoord, MiruPartitionCoordInfo> coordToInfo = Maps.newConcurrentMap();

    @Override
    public Optional<MiruPartitionCoordInfo> get(MiruPartitionCoord coord) {
        return Optional.fromNullable(coordToInfo.get(coord));
    }

    @Override
    public void put(MiruPartitionCoord partitionCoord, MiruPartitionCoordInfo partitionCoordInfo) {
        coordToInfo.put(partitionCoord, partitionCoordInfo);
    }

    @Override
    public Iterator<MiruPartitionCoord> getKeysIterator() {
        return coordToInfo.keySet().iterator();
    }

}
