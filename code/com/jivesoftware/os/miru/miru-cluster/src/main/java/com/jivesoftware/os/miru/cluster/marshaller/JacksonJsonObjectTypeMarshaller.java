package com.jivesoftware.os.miru.cluster.marshaller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;

public class JacksonJsonObjectTypeMarshaller<T> implements TypeMarshaller<T> {

    private final Class<T> marshalledClass;
    private final ObjectMapper objectMapper;

    public JacksonJsonObjectTypeMarshaller(Class<T> marshalledClass, ObjectMapper objectMapper) {
        this.marshalledClass = marshalledClass;
        this.objectMapper = objectMapper;
    }

    @Override
    public T fromBytes(byte[] bytes) throws Exception {
        return objectMapper.readValue(bytes, marshalledClass);
    }

    @Override
    public byte[] toBytes(T object) throws Exception {
        return objectMapper.writeValueAsBytes(object);
    }

    @Override
    public T fromLexBytes(byte[] bytes) throws Exception {
        return fromBytes(bytes);
    }

    @Override
    public byte[] toLexBytes(T object) throws Exception {
        return toBytes(object);
    }
}
