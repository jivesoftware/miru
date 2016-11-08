package com.jivesoftware.os.miru.bot.deployable;

import com.jivesoftware.os.miru.bot.deployable.MiruBotDistinctsInitializer.MiruBotDistinctsConfig;
import com.jivesoftware.os.miru.bot.deployable.MiruBotUniquesInitializer.MiruBotUniquesConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.merlin.config.BindInterfaceToConfiguration;

class MiruBotBucketRequest {

    private final int readTimeRangeFactor;
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
            @JsonProperty("readTimeRangeFactor") int readTimeRangeFactor,
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
        this.readTimeRangeFactor = readTimeRangeFactor;
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

    static MiruBotDistinctsConfig genDistinctsConfig(MiruBotBucketRequest miruBotBucketRequest) {
        MiruBotDistinctsConfig res =
                BindInterfaceToConfiguration.bindDefault(MiruBotDistinctsConfig.class);

        if (miruBotBucketRequest != null) {
            if (miruBotBucketRequest.readTimeRangeFactor > 0) {
                res.setReadTimeRangeFactor(miruBotBucketRequest.readTimeRangeFactor);
            }

            if (miruBotBucketRequest.writeHesitationFactor > 0) {
                res.setWriteHesitationFactor(miruBotBucketRequest.writeHesitationFactor);
            }

            if (miruBotBucketRequest.valueSizeFactor > 0) {
                res.setValueSizeFactor(miruBotBucketRequest.valueSizeFactor);
            }

            if (miruBotBucketRequest.birthRateFactor > 0) {
                res.setBirthRateFactor(miruBotBucketRequest.birthRateFactor);
            }

            if (miruBotBucketRequest.failureRetryWaitMs> 0) {
                res.setFailureRetryWaitMs(miruBotBucketRequest.failureRetryWaitMs);
            }

            if (miruBotBucketRequest.readFrequency > 0) {
                res.setReadFrequency(miruBotBucketRequest.readFrequency);
            }

            if (miruBotBucketRequest.batchWriteCountFactor > 0) {
                res.setBatchWriteCountFactor(miruBotBucketRequest.batchWriteCountFactor);
            }

            if (miruBotBucketRequest.batchWriteFrequency > 0) {
                res.setBatchWriteFrequency(miruBotBucketRequest.batchWriteFrequency);
            }

            if (miruBotBucketRequest.numberOfFields > 0) {
                res.setNumberOfFields(miruBotBucketRequest.numberOfFields);
            }

            if (miruBotBucketRequest.botBucketSeed > 0) {
                res.setBotBucketSeed(miruBotBucketRequest.botBucketSeed);
            }

            if (miruBotBucketRequest.writeReadPauseMs > 0L) {
                res.setWriteReadPauseMs(miruBotBucketRequest.writeReadPauseMs);
            }

            if (miruBotBucketRequest.runtimeMs > 0L) {
                res.setRuntimeMs(miruBotBucketRequest.runtimeMs);
            }
        }

        return res;
    }

    static MiruBotUniquesConfig genUniquesConfig(MiruBotBucketRequest miruBotBucketRequest) {
        return (MiruBotUniquesConfig) genDistinctsConfig(miruBotBucketRequest);
    }

    }
