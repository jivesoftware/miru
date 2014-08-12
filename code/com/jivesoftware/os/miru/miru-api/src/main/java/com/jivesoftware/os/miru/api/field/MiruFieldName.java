package com.jivesoftware.os.miru.api.field;

public enum MiruFieldName {

    OBJECT_ID("objectId"),
    VIEW_CLASS_NAME("viewClassName"),
    VERB_SUBJECT_CLASS_NAME("verbSubjectClassName"),
    META_CLASS_NAME("metaClassName"),
    META_ID("metaId"),
    AUTHZ("authz"),
    AUTHOR_ID("authorId"),
    CONTAINER_ID("containerId"),
    CONTAINER_IDS("containerIds"),
    ACTIVITY_PARENT("activityParent"),
    MENTIONED_USER_IDS("mentionedUserIds"),
    MENTIONED_CONTAINER_IDS("mentionedContainerIds"),
    TAG_IDS("tagIds"),
    TAGS("tags"),
    PARTICIPANT_IDS("participantIds"),
    STATUS("status");

    private final String fieldName;

    private MiruFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public static MiruFieldName fieldNameToMiruFieldName(String fieldName) {
        for (MiruFieldName miruFieldName : values()) {
            if (miruFieldName.fieldName.equals(fieldName)) {
                return miruFieldName;
            }
        }
        return null;
    }
}
