package com.jivesoftware.os.miru.service.schema;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import com.jivesoftware.os.miru.service.index.MiruFieldDefinition;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultMiruSchemaDefinition {

    public static final MiruFieldDefinition[] SCHEMA;

    static {
        AtomicInteger fieldIndex = new AtomicInteger();
        List<MiruFieldDefinition> schema = Lists.newArrayList();
        
        schema.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.OBJECT_ID.getFieldName()));
        schema.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.VIEW_CLASS_NAME.getFieldName()));
        schema.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.VERB_SUBJECT_CLASS_NAME.getFieldName()));

        schema.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.META_CLASS_NAME.getFieldName()));
        schema.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.META_ID.getFieldName()));

        schema.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.PARTICIPANT_IDS.getFieldName()));

        schema.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.ACTIVITY_PARENT.getFieldName()));
        schema.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.CONTAINER_ID.getFieldName()));
        schema.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.AUTHOR_ID.getFieldName()));

        schema.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.MENTIONED_CONTAINER_IDS.getFieldName()));
        schema.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.MENTIONED_USER_IDS.getFieldName()));
        schema.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.TAG_IDS.getFieldName()));
        schema.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.TAGS.getFieldName()));

        schema.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.STATUS.getFieldName()));
        schema.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.CONTAINER_IDS.getFieldName()));

        SCHEMA = schema.toArray(new MiruFieldDefinition[schema.size()]);
    }
}
