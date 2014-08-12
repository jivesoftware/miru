package com.jivesoftware.os.miru.service.schema;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

/**
 *
 * @author jonathan
 */
public class MiruSchema {

    private final Map<String, Integer> fieldNameToId;

    public MiruSchema(ImmutableMap<String, Integer> fieldNameToId) {
        this.fieldNameToId = fieldNameToId;
    }

    public int getFieldId(String fieldName) {
        Integer fieldId = fieldNameToId.get(fieldName);
        if (fieldId == null) {
            return -1;
        }
        return fieldId;
    }

    public int fieldCount() {
        return fieldNameToId.size();
    }
}
