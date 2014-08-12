package com.jivesoftware.os.miru.service.schema;

import com.google.common.collect.ImmutableMap;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultMiruSchemaDefinition {

    public static final ImmutableMap<String, Integer> SCHEMA;

    static {
        AtomicInteger fieldIndex = new AtomicInteger();
        SCHEMA = ImmutableMap.<String, Integer>builder()
            .put(MiruFieldName.OBJECT_ID.getFieldName(), fieldIndex.getAndIncrement())
            .put(MiruFieldName.VIEW_CLASS_NAME.getFieldName(), fieldIndex.getAndIncrement())
            .put(MiruFieldName.VERB_SUBJECT_CLASS_NAME.getFieldName(), fieldIndex.getAndIncrement())

            .put(MiruFieldName.META_CLASS_NAME.getFieldName(), fieldIndex.getAndIncrement())
            .put(MiruFieldName.META_ID.getFieldName(), fieldIndex.getAndIncrement())

            .put(MiruFieldName.PARTICIPANT_IDS.getFieldName(), fieldIndex.getAndIncrement())

            .put(MiruFieldName.ACTIVITY_PARENT.getFieldName(), fieldIndex.getAndIncrement())
            .put(MiruFieldName.CONTAINER_ID.getFieldName(), fieldIndex.getAndIncrement())
            .put(MiruFieldName.AUTHOR_ID.getFieldName(), fieldIndex.getAndIncrement())

            .put(MiruFieldName.MENTIONED_CONTAINER_IDS.getFieldName(), fieldIndex.getAndIncrement())
            .put(MiruFieldName.MENTIONED_USER_IDS.getFieldName(), fieldIndex.getAndIncrement())
            .put(MiruFieldName.TAG_IDS.getFieldName(), fieldIndex.getAndIncrement())
            .put(MiruFieldName.TAGS.getFieldName(), fieldIndex.getAndIncrement())

            .put(MiruFieldName.STATUS.getFieldName(), fieldIndex.getAndIncrement())
            .put(MiruFieldName.CONTAINER_IDS.getFieldName(), fieldIndex.getAndIncrement())
            .build();
    }
}
