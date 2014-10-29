/*
 * Copyright 2014 Jive Software Inc.
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
package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class NoOpMiruIndexRepairs implements MiruIndexRepairs {

    @Override
    public void repaired(MiruPartitionAccessor.IndexStrategy strategy, MiruPartitionCoord coord, List<MiruActivityAndId<MiruActivity>> indexables) {
    }

    @Override
    public void current(MiruPartitionAccessor.IndexStrategy strategy, MiruPartitionCoord coord) {
    }

}
