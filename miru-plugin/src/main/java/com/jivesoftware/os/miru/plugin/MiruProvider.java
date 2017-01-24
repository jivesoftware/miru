package com.jivesoftware.os.miru.plugin;

import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruHostSelectiveStrategy;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.backfill.MiruJustInTimeBackfillerizer;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.plugin.IndexCloseCallback;
import com.jivesoftware.os.miru.plugin.plugin.IndexCommitCallback;
import com.jivesoftware.os.miru.plugin.plugin.IndexOpenCallback;
import com.jivesoftware.os.miru.plugin.query.MiruQueryParser;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.routing.bird.health.HealthCheck;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import java.util.Map;
import org.merlin.config.Config;

/**
 *
 */
public interface MiruProvider<T extends Miru> {

    MiruStats getStats();

    T getMiru(MiruTenantId tenantId);

    MiruHost getHost();

    MiruActivityInternExtern getActivityInternExtern(MiruTenantId tenantId);

    MiruJustInTimeBackfillerizer getBackfillerizer(MiruTenantId tenantId);

    MiruTermComposer getTermComposer();

    MiruQueryParser getQueryParser(String defaultField);

    <R extends MiruRemotePartition<?, ?, ?>> R getRemotePartition(Class<R> remotePartitionClass);

    TenantAwareHttpClient<String> getReaderHttpClient();

    TenantAwareHttpClient<String> getCatwalkHttpClient();

    TenantAwareHttpClient<String> getTenantAwareHttpClient(String serviceName, int socketTimeoutMillis);

    Map<MiruHost, MiruHostSelectiveStrategy> getReaderStrategyCache();

    <C extends Config> C getConfig(Class<C> configClass);

    void addHealthCheck(HealthCheck healthCheck);

    void addIndexOpenCallback(IndexOpenCallback callback);

    void addIndexCommitCallback(IndexCommitCallback callback);

    void addIndexCloseCallback(IndexCloseCallback callback);

    void removeIndexOpenCallback(IndexOpenCallback callback);

    void removeIndexCommitCallback(IndexCommitCallback callback);

    void removeIndexCloseCallback(IndexCloseCallback callback);
}
