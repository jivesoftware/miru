package com.jivesoftware.os.miru.sync.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by jonathan.colt on 12/22/16.
 */
public class MiruSyncSenderConfig {


    public final String name;
    public final boolean enabled;
    public final String senderScheme;
    public final String senderHost;
    public final int senderPort;
    public final long syncIntervalMillis;
    public final long forwardSyncDelayMillis;
    public final int batchSize;
    public final String oAuthConsumerKey;
    public final String oAuthConsumerSecret;
    public final String oAuthConsumerMethod;
    public final boolean allowSelfSignedCerts;

    @JsonCreator
    public MiruSyncSenderConfig(@JsonProperty("name") String name,
        @JsonProperty("enabled") boolean enabled,
        @JsonProperty("senderScheme") String senderScheme,
        @JsonProperty("senderHost")  String senderHost,
        @JsonProperty("senderPort") int senderPort,
        @JsonProperty("syncIntervalMillis") long syncIntervalMillis,
        @JsonProperty("forwardSyncDelayMillis") long forwardSyncDelayMillis,
        @JsonProperty("batchSize") int batchSize,
        @JsonProperty("oAuthConsumerKey") String oAuthConsumerKey,
        @JsonProperty("oAuthConsumerSecret") String oAuthConsumerSecret,
        @JsonProperty("oAuthConsumerMethod") String oAuthConsumerMethod,
        @JsonProperty("allowSelfSignedCerts") boolean allowSelfSignedCerts) {

        this.name = name;
        this.enabled = enabled;
        this.senderScheme = senderScheme;
        this.senderHost = senderHost;
        this.senderPort = senderPort;
        this.syncIntervalMillis = syncIntervalMillis;
        this.forwardSyncDelayMillis = forwardSyncDelayMillis;
        this.batchSize = batchSize;
        this.oAuthConsumerKey = oAuthConsumerKey;
        this.oAuthConsumerSecret = oAuthConsumerSecret;
        this.oAuthConsumerMethod = oAuthConsumerMethod;
        this.allowSelfSignedCerts = allowSelfSignedCerts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MiruSyncSenderConfig that = (MiruSyncSenderConfig) o;

        if (enabled != that.enabled) {
            return false;
        }
        if (senderPort != that.senderPort) {
            return false;
        }
        if (syncIntervalMillis != that.syncIntervalMillis) {
            return false;
        }
        if (forwardSyncDelayMillis != that.forwardSyncDelayMillis) {
            return false;
        }
        if (batchSize != that.batchSize) {
            return false;
        }
        if (allowSelfSignedCerts != that.allowSelfSignedCerts) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (senderScheme != null ? !senderScheme.equals(that.senderScheme) : that.senderScheme != null) {
            return false;
        }
        if (senderHost != null ? !senderHost.equals(that.senderHost) : that.senderHost != null) {
            return false;
        }
        if (oAuthConsumerKey != null ? !oAuthConsumerKey.equals(that.oAuthConsumerKey) : that.oAuthConsumerKey != null) {
            return false;
        }
        if (oAuthConsumerSecret != null ? !oAuthConsumerSecret.equals(that.oAuthConsumerSecret) : that.oAuthConsumerSecret != null) {
            return false;
        }
        return oAuthConsumerMethod != null ? oAuthConsumerMethod.equals(that.oAuthConsumerMethod) : that.oAuthConsumerMethod == null;

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (enabled ? 1 : 0);
        result = 31 * result + (senderScheme != null ? senderScheme.hashCode() : 0);
        result = 31 * result + (senderHost != null ? senderHost.hashCode() : 0);
        result = 31 * result + senderPort;
        result = 31 * result + (int) (syncIntervalMillis ^ (syncIntervalMillis >>> 32));
        result = 31 * result + (int) (forwardSyncDelayMillis ^ (forwardSyncDelayMillis >>> 32));
        result = 31 * result + batchSize;
        result = 31 * result + (oAuthConsumerKey != null ? oAuthConsumerKey.hashCode() : 0);
        result = 31 * result + (oAuthConsumerSecret != null ? oAuthConsumerSecret.hashCode() : 0);
        result = 31 * result + (oAuthConsumerMethod != null ? oAuthConsumerMethod.hashCode() : 0);
        result = 31 * result + (allowSelfSignedCerts ? 1 : 0);
        return result;
    }
}
