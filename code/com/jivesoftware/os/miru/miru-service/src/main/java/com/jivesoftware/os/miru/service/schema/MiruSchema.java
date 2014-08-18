package com.jivesoftware.os.miru.service.schema;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 * @author jonathan
 */
public class MiruSchema {

    private final Map<String, Integer> fieldNameToId;
    private final Map<String, List<String>> fieldNamesBlooms;

    public MiruSchema(ImmutableMap<String, Integer> fieldNameToId) {
        this(fieldNameToId, Collections.<String, List<String>>emptyMap());
    }

    public MiruSchema(ImmutableMap<String, Integer> fieldNameToId, Map<String, List<String>> fieldNamesBlooms) {
        this.fieldNameToId = fieldNameToId;
        this.fieldNamesBlooms = fieldNamesBlooms;
    }

    public int getFieldId(String fieldName) {
        Integer fieldId = fieldNameToId.get(fieldName);
        if (fieldId == null) {
            return -1;
        }
        return fieldId;
    }

    public Optional<List<String>> getFieldsBlooms(String fieldName) {
        return Optional.fromNullable(fieldNamesBlooms.get(fieldName));
    }

    public int fieldCount() {
        return fieldNameToId.size();
    }
}
