/*
 * Copyright 2016 jonathan.colt.
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
package com.jivesoftware.os.miru.stream.plugins.strut;

import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.Map;

/**
 * @author jonathan.colt
 */
public class StrutModel {

    final Map<StrutModelCache.StrutModelKey, StrutModelCache.ModelScore>[] model;
    public final long modelCount;
    public final long totalCount;
    public final int[] numberOfModels;

    public StrutModel(Map<StrutModelCache.StrutModelKey, StrutModelCache.ModelScore>[] model,
        long modelCount,
        long totalCount,
        int[] numberOfModels) {
        this.model = model;
        this.modelCount = modelCount;
        this.totalCount = totalCount;
        this.numberOfModels = numberOfModels;
    }

    public StrutModelCache.ModelScore score(int featureId, MiruTermId[] values) {
        return model[featureId].get(new StrutModelCache.StrutModelKey(values));
    }

}
