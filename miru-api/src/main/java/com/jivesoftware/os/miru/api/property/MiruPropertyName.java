package com.jivesoftware.os.miru.api.property;

public enum MiruPropertyName {

    READ_STREAMIDS("readStreamIds");

    private final String propertyName;

    private MiruPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public static MiruPropertyName fieldNameToMiruFieldName(String propertyName) {
        for (MiruPropertyName miruPropertyName : values()) {
            if (miruPropertyName.propertyName.equals(propertyName)) {
                return miruPropertyName;
            }
        }
        return null;
    }
}
