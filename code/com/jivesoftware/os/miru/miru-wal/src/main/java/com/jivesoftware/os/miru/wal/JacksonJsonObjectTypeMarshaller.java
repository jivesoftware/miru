package com.jivesoftware.os.miru.wal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;

public class JacksonJsonObjectTypeMarshaller<T> implements TypeMarshaller<T> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Class<T> marshalledClass;

    public JacksonJsonObjectTypeMarshaller(Class<T> marshalledClass) {
        this.marshalledClass = marshalledClass;
    }

    @Override
    public T fromBytes(byte[] bytes) throws Exception {
        return OBJECT_MAPPER.readValue(bytes, marshalledClass);
    }

    @Override
    public byte[] toBytes(T object) throws Exception {
        return OBJECT_MAPPER.writeValueAsBytes(object);
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
