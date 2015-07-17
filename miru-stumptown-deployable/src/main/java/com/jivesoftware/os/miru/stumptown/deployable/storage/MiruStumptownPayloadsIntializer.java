package com.jivesoftware.os.miru.stumptown.deployable.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.MiruTenantIdMarshaller;
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
public class MiruStumptownPayloadsIntializer {

    public MiruStumptownPayloadsIntializer() {
    }

    public MiruStumptownPayloads initialize(String tableNameSpace,
        RowColumnValueStoreInitializer<? extends Exception> rowColumnValueStoreInitializer,
        ObjectMapper mapper) throws IOException {
        RowColumnValueStore<MiruTenantId, Long, String, byte[], ? extends Exception> stumptownPayloadTable =
            rowColumnValueStoreInitializer.initialize(
                tableNameSpace,
                "stumptown.payloads",
                "cf",
                new DefaultRowColumnValueStoreMarshaller<>(
                    new MiruTenantIdMarshaller(),
                    new SaltingDelegatingMarshaller<>(new LongTypeMarshaller()),
                    new StringTypeMarshaller(),
                    new ByteArrayTypeMarshaller()),
                new CurrentTimestamper()
            );
        return new MiruStumptownPayloads(mapper, stumptownPayloadTable);
    }
}
