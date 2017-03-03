package com.jivesoftware.os.miru.bot.deployable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.bot.deployable.MiruBotDistinctsInitializer.MiruBotDistinctsConfig;
import com.jivesoftware.os.miru.bot.deployable.MiruBotUniquesInitializer.MiruBotUniquesConfig;
import org.merlin.config.BindInterfaceToConfiguration;

class MiruBotBucketRequest {

    private final int readTimeRange;
    private final int writeHesitationFactor;
    private final int valueSizeFactor;
    private final long failureRetryWaitMs;
    private final int birthRateFactor;
    private final int readFrequency;
    private final int batchWriteCountFactor;
    private final int batchWriteFrequency;
    private final int numberOfFields;
    private final int botBucketSeed;
    private final long writeReadPauseMs;
    private final long runtimeMs;

    @JsonCreator
    public MiruBotBucketRequest(
        @JsonProperty("readTimeRange") int readTimeRange,
        @JsonProperty("writeHesitationFactor") int writeHesitationFactor,
        @JsonProperty("valueSizeFactor") int valueSizeFactor,
        @JsonProperty("failureRetryWaitMs") long failureRetryWaitMs,
        @JsonProperty("birthRateFactor") int birthRateFactor,
        @JsonProperty("readFrequency") int readFrequency,
        @JsonProperty("batchWriteCountFactor") int batchWriteCountFactor,
        @JsonProperty("batchWriteFrequency") int batchWriteFrequency,
        @JsonProperty("numberOfFields") int numberOfFields,
        @JsonProperty("botBucketSeed") int botBucketSeed,
        @JsonProperty("writeReadPauseMs") long writeReadPauseMs,
        @JsonProperty("runtimeMs") long runtimeMs) {
        this.readTimeRange = readTimeRange;
        this.writeHesitationFactor = writeHesitationFactor;
        this.valueSizeFactor = valueSizeFactor;
        this.failureRetryWaitMs = failureRetryWaitMs;
        this.birthRateFactor = birthRateFactor;
        this.readFrequency = readFrequency;
        this.batchWriteCountFactor = batchWriteCountFactor;
        this.batchWriteFrequency = batchWriteFrequency;
        this.numberOfFields = numberOfFields;
        this.botBucketSeed = botBucketSeed;
        this.writeReadPauseMs = writeReadPauseMs;
        this.runtimeMs = runtimeMs;
    }

    static void genConfig(
        MiruBotBucketRequest miruBotBucketRequest,
        MiruBotDistinctsConfig config) {
        if (miruBotBucketRequest != null) {
            if (miruBotBucketRequest.readTimeRange > 0) {
                config.setReadTimeRange(miruBotBucketRequest.readTimeRange);
            }

            if (miruBotBucketRequest.writeHesitationFactor > 0) {
                config.setWriteHesitationFactor(miruBotBucketRequest.writeHesitationFactor);
            }

            if (miruBotBucketRequest.valueSizeFactor > 0) {
                config.setValueSizeFactor(miruBotBucketRequest.valueSizeFactor);
            }

            if (miruBotBucketRequest.birthRateFactor > 0) {
                config.setBirthRateFactor(miruBotBucketRequest.birthRateFactor);
            }

            if (miruBotBucketRequest.failureRetryWaitMs > 0) {
                config.setFailureRetryWaitMs(miruBotBucketRequest.failureRetryWaitMs);
            }

            if (miruBotBucketRequest.readFrequency > 0) {
                config.setReadFrequency(miruBotBucketRequest.readFrequency);
            }

            if (miruBotBucketRequest.batchWriteCountFactor > 0) {
                config.setBatchWriteCountFactor(miruBotBucketRequest.batchWriteCountFactor);
            }

            if (miruBotBucketRequest.batchWriteFrequency > 0) {
                config.setBatchWriteFrequency(miruBotBucketRequest.batchWriteFrequency);
            }

            if (miruBotBucketRequest.numberOfFields > 0) {
                config.setNumberOfFields(miruBotBucketRequest.numberOfFields);
            }

            if (miruBotBucketRequest.botBucketSeed > 0) {
                config.setBotBucketSeed(miruBotBucketRequest.botBucketSeed);
            }

            if (miruBotBucketRequest.writeReadPauseMs > 0L) {
                config.setWriteReadPauseMs(miruBotBucketRequest.writeReadPauseMs);
            }

            if (miruBotBucketRequest.runtimeMs > 0L) {
                config.setRuntimeMs(miruBotBucketRequest.runtimeMs);
            }
        }
    }

    static MiruBotDistinctsConfig genDistinctsConfig(MiruBotBucketRequest request) {
        MiruBotDistinctsConfig res =
            BindInterfaceToConfiguration.bindDefault(MiruBotDistinctsConfig.class);
        genConfig(request, res);
        return res;
    }

    static MiruBotUniquesConfig genUniquesConfig(MiruBotBucketRequest request) {
        MiruBotUniquesConfig res =
            BindInterfaceToConfiguration.bindDefault(MiruBotUniquesConfig.class);
        genConfig(request, res);
        return res;
    }

}
