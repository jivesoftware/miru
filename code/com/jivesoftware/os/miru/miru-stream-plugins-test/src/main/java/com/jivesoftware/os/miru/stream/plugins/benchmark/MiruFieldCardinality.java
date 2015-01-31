package com.jivesoftware.os.miru.stream.plugins.benchmark;

import com.google.common.collect.ImmutableMap;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import java.util.Map;

public enum MiruFieldCardinality {
    LOW {
        private final Map<MiruFieldName, Double> fieldNameToCardinalityPercentage = ImmutableMap.<MiruFieldName, Double>builder()
            .put(MiruFieldName.ACTIVITY_PARENT, 0.5)
            .put(MiruFieldName.AUTHOR_ID, 0.1)
            .put(MiruFieldName.CONTAINER_ID, 0.1)
            .put(MiruFieldName.MENTIONED_CONTAINER_IDS, 0.1)
            .put(MiruFieldName.MENTIONED_USER_IDS, 0.1)
            .put(MiruFieldName.META_CLASS_NAME, 0.1)
            .put(MiruFieldName.META_ID, 0.1)
            .put(MiruFieldName.OBJECT_ID, 1.0)
            .put(MiruFieldName.PARTICIPANT_IDS, 0.1)
            .put(MiruFieldName.STATUS, 0.1)
            .put(MiruFieldName.TAG_IDS, 0.1)
            .put(MiruFieldName.VERB_SUBJECT_CLASS_NAME, 0.1)
            .put(MiruFieldName.VIEW_CLASS_NAME, 0.1)
            .build();

        @Override
        int getCardinality(MiruFieldName miruFieldName, int expectedTotal) {
            int cardinality = (int) (fieldNameToCardinalityPercentage.get(miruFieldName) * expectedTotal);
            return cardinality == 0 ? 1 : cardinality;
        }
    },
    MEDIUM {
        private final Map<MiruFieldName, Double> fieldNameToCardinalityPercentage = ImmutableMap.<MiruFieldName, Double>builder()
            .put(MiruFieldName.ACTIVITY_PARENT, 0.5)
            .put(MiruFieldName.AUTHOR_ID, 0.4)
            .put(MiruFieldName.CONTAINER_ID, 0.4)
            .put(MiruFieldName.MENTIONED_CONTAINER_IDS, 0.4)
            .put(MiruFieldName.MENTIONED_USER_IDS, 0.4)
            .put(MiruFieldName.META_CLASS_NAME, 0.4)
            .put(MiruFieldName.META_ID, 0.4)
            .put(MiruFieldName.OBJECT_ID, 1.0)
            .put(MiruFieldName.PARTICIPANT_IDS, 0.4)
            .put(MiruFieldName.STATUS, 0.4)
            .put(MiruFieldName.TAG_IDS, 0.4)
            .put(MiruFieldName.VERB_SUBJECT_CLASS_NAME, 0.4)
            .put(MiruFieldName.VIEW_CLASS_NAME, 0.4)
            .build();

        @Override
        int getCardinality(MiruFieldName miruFieldName, int expectedTotal) {
            int cardinality = (int) (fieldNameToCardinalityPercentage.get(miruFieldName) * expectedTotal);
            return cardinality == 0 ? 1 : cardinality;
        }
    },
    HIGH {
        private final Map<MiruFieldName, Double> fieldNameToCardinalityPercentage = ImmutableMap.<MiruFieldName, Double>builder()
            .put(MiruFieldName.ACTIVITY_PARENT, 0.5)
            .put(MiruFieldName.AUTHOR_ID, 0.9)
            .put(MiruFieldName.CONTAINER_ID, 0.9)
            .put(MiruFieldName.MENTIONED_CONTAINER_IDS, 0.9)
            .put(MiruFieldName.MENTIONED_USER_IDS, 0.9)
            .put(MiruFieldName.META_CLASS_NAME, 0.9)
            .put(MiruFieldName.META_ID, 0.9)
            .put(MiruFieldName.OBJECT_ID, 1.0)
            .put(MiruFieldName.PARTICIPANT_IDS, 0.9)
            .put(MiruFieldName.STATUS, 0.9)
            .put(MiruFieldName.TAG_IDS, 0.9)
            .put(MiruFieldName.VERB_SUBJECT_CLASS_NAME, 0.9)
            .put(MiruFieldName.VIEW_CLASS_NAME, 0.9)
            .build();

        @Override
        int getCardinality(MiruFieldName miruFieldName, int expectedTotal) {
            int cardinality = (int) (fieldNameToCardinalityPercentage.get(miruFieldName) * expectedTotal);
            return cardinality == 0 ? 1 : cardinality;
        }
    };

    abstract int getCardinality(MiruFieldName miruFieldName, int expectedTotal);
}
