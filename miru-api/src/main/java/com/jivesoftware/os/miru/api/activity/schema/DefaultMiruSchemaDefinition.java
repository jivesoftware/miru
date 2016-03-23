package com.jivesoftware.os.miru.api.activity.schema;

import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Type;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import com.jivesoftware.os.miru.api.property.MiruPropertyName;

public class DefaultMiruSchemaDefinition {

    public static final MiruFieldDefinition[] FIELDS = new MiruFieldDefinition[] {
        new MiruFieldDefinition(0, MiruFieldName.OBJECT_ID.getFieldName(), Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
        new MiruFieldDefinition(1, MiruFieldName.VIEW_CLASS_NAME.getFieldName(), Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
        new MiruFieldDefinition(2, MiruFieldName.VERB_SUBJECT_CLASS_NAME.getFieldName(), Type.singleTerm, MiruFieldDefinition.Prefix.NONE),

        new MiruFieldDefinition(3, MiruFieldName.META_CLASS_NAME.getFieldName(), Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
        new MiruFieldDefinition(4, MiruFieldName.META_ID.getFieldName(), Type.singleTerm, MiruFieldDefinition.Prefix.NONE),

        new MiruFieldDefinition(5, MiruFieldName.PARTICIPANT_IDS.getFieldName(), Type.singleTerm, MiruFieldDefinition.Prefix.NONE),

        new MiruFieldDefinition(6, MiruFieldName.ACTIVITY_PARENT.getFieldName(), Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
        new MiruFieldDefinition(7, MiruFieldName.CONTAINER_ID.getFieldName(), Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
        new MiruFieldDefinition(8, MiruFieldName.AUTHOR_ID.getFieldName(), Type.singleTerm, MiruFieldDefinition.Prefix.NONE),

        new MiruFieldDefinition(9, MiruFieldName.MENTIONED_CONTAINER_IDS.getFieldName(), Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
        new MiruFieldDefinition(10, MiruFieldName.MENTIONED_USER_IDS.getFieldName(), Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
        new MiruFieldDefinition(11, MiruFieldName.TAG_IDS.getFieldName(), Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
        new MiruFieldDefinition(12, MiruFieldName.TAGS.getFieldName(), Type.singleTerm, MiruFieldDefinition.Prefix.NONE),

        new MiruFieldDefinition(13, MiruFieldName.STATUS.getFieldName(), Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
        new MiruFieldDefinition(14, MiruFieldName.CONTAINER_IDS.getFieldName(), Type.singleTerm, MiruFieldDefinition.Prefix.NONE)
    };

    public static final MiruPropertyDefinition[] PROPERTIES = new MiruPropertyDefinition[] {
        new MiruPropertyDefinition(0, MiruPropertyName.READ_STREAMIDS.getPropertyName())
    };

    private DefaultMiruSchemaDefinition() {
    }

}
