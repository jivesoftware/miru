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
package com.jivesoftware.os.miru.sea.anomaly.deployable.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.marshaller.MiruTenantIdMarshaller;
import com.jivesoftware.os.rcvs.api.DefaultRowColumnValueStoreMarshaller;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.rcvs.marshall.id.SaltingDelegatingMarshaller;
import com.jivesoftware.os.rcvs.marshall.primatives.ByteArrayTypeMarshaller;
import com.jivesoftware.os.rcvs.marshall.primatives.LongTypeMarshaller;
import com.jivesoftware.os.rcvs.marshall.primatives.StringTypeMarshaller;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class MiruSeaAnomalyPayloadsIntializer {

    public MiruSeaAnomalyPayloadsIntializer() {
    }

    public MiruSeaAnomalyPayloads initialize(String tableNameSpace,
        RowColumnValueStoreInitializer<? extends Exception> rowColumnValueStoreInitializer,
        ObjectMapper mapper) throws IOException {
        RowColumnValueStore<MiruTenantId, Long, String, byte[], ? extends Exception> seaAnomalyPayloadTable =
            rowColumnValueStoreInitializer.initialize(
                tableNameSpace,
                "seaAnomaly.payloads",
                "cf",
                new DefaultRowColumnValueStoreMarshaller<>(
                    new MiruTenantIdMarshaller(),
                    new SaltingDelegatingMarshaller<>(new LongTypeMarshaller()),
                    new StringTypeMarshaller(),
                    new ByteArrayTypeMarshaller()),
                new CurrentTimestamper()
            );
        return new MiruSeaAnomalyPayloads(mapper, seaAnomalyPayloadTable);
    }
}
