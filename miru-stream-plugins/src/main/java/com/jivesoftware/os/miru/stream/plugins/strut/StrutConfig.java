package com.jivesoftware.os.miru.stream.plugins.strut;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.DoubleDefault;
import org.merlin.config.defaults.FloatDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan.colt
 */
public interface StrutConfig extends Config {

    @BooleanDefault(true)
    boolean getModelCacheEnabled();

    @LongDefault(60 * 60 * 1_000L)
    long getModelCacheExpirationInMillis();

    @LongDefault(1024)
    long getModelCacheMaxSize();

    @IntDefault(24)
    int getAsyncThreadPoolSize();

    @IntDefault(10_000)
    int getMaxTermIdsPerRequest();

    @IntDefault(1024 * 1024)
    int getMaxHeapPressureInBytes();

    @IntDefault(24)
    int getQueueStripeCount();

    @LongDefault(60_000)
    long getQueueConsumeIntervalMillis();

    @IntDefault(1_000)
    int getCatwalkTopNValuesPerFeature();

    @IntDefault(1_000)
    int getCatwalkTopNTermsPerNumerator();

    @IntDefault(100)
    int getStrutTopNValuesPerFeature();

    @IntDefault(4)
    int getCatwalkSolverPoolSize();

    @BooleanDefault(false)
    boolean getAllowImmediateStrutRescore();

    @IntDefault(100)
    int getGatherBatchSize();

    @IntDefault(1)
    int getGatherThreadPoolSize();

    @BooleanDefault(true)
    boolean getShareScores();

    @BooleanDefault(false)
    boolean getVerboseLogging();

    @DoubleDefault(1d)
    double getScoresHashIndexLoadFactor();

    @BooleanDefault(false)
    boolean getParallelSolverEnabled();

    @FloatDefault(0.0001f)
    float getNilScoreThreshold();

    @StringDefault("")
    String getVerboseModelIds();
}
