package com.jivesoftware.os.miru.cluster.marshaller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;

/**
 *
 */
public class MiruSchemaMarshaller implements TypeMarshaller<MiruSchema> {

    private final ObjectMapper objectMapper;

    public MiruSchemaMarshaller(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public MiruSchema fromBytes(byte[] bytes) throws Exception {
        return objectMapper.readValue(bytes, MiruSchema.class);
    }

    @Override
    public byte[] toBytes(MiruSchema miruSchema) throws Exception {
        return new byte[0];
    }

    @Override
    public MiruSchema fromLexBytes(byte[] bytes) throws Exception {
        return null;
    }

    @Override
    public byte[] toLexBytes(MiruSchema miruSchema) throws Exception {
        return new byte[0];
    }
}
