package com.jivesoftware.os.miru.api.activity.schema;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import com.jivesoftware.os.miru.api.property.MiruPropertyName;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultMiruSchemaDefinition {

    public static final MiruFieldDefinition[] FIELDS;

    static {
        AtomicInteger fieldIndex = new AtomicInteger();
        List<MiruFieldDefinition> fieldDefinitions = Lists.newArrayList();
        
        fieldDefinitions.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.OBJECT_ID.getFieldName()));
        fieldDefinitions.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.VIEW_CLASS_NAME.getFieldName()));
        fieldDefinitions.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.VERB_SUBJECT_CLASS_NAME.getFieldName()));

        fieldDefinitions.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.META_CLASS_NAME.getFieldName()));
        fieldDefinitions.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.META_ID.getFieldName()));

        fieldDefinitions.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.PARTICIPANT_IDS.getFieldName()));

        fieldDefinitions.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.ACTIVITY_PARENT.getFieldName()));
        fieldDefinitions.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.CONTAINER_ID.getFieldName()));
        fieldDefinitions.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.AUTHOR_ID.getFieldName()));

        fieldDefinitions.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.MENTIONED_CONTAINER_IDS.getFieldName()));
        fieldDefinitions.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.MENTIONED_USER_IDS.getFieldName()));
        fieldDefinitions.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.TAG_IDS.getFieldName()));
        fieldDefinitions.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.TAGS.getFieldName()));

        fieldDefinitions.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.STATUS.getFieldName()));
        fieldDefinitions.add(new MiruFieldDefinition(fieldIndex.getAndIncrement(), MiruFieldName.CONTAINER_IDS.getFieldName()));

        FIELDS = fieldDefinitions.toArray(new MiruFieldDefinition[fieldDefinitions.size()]);
    }
    
    public static final MiruPropertyDefinition[] PROPERTIES;

    static {
        AtomicInteger propIndex = new AtomicInteger();
        List<MiruPropertyDefinition> propDefinitions = Lists.newArrayList();

        propDefinitions.add(new MiruPropertyDefinition(propIndex.getAndIncrement(), MiruPropertyName.READ_STREAMIDS.getPropertyName()));

        PROPERTIES = propDefinitions.toArray(new MiruPropertyDefinition[propDefinitions.size()]);
    }

}
