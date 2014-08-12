package com.jivesoftware.os.miru.service;

import com.google.inject.AbstractModule;
import com.google.inject.util.Providers;
import com.jivesoftware.jive.entitlements.entitlements.api.EntitlementExpressionProvider;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.MiruWriter;

public class MiruReaderWriterInMemoryModule extends AbstractModule {

    @Override
    protected void configure() {
        install(new MiruServiceInMemoryModule());

        // TODO -- Are these correct values?
//        ImmutableSet<Permission> perms = ImmutableSet.<Permission>of(AllPermissions.VIEW);
//        ImmutableMap<String, String> config = ImmutableMap.of();
//        EntitlementSystem entitlementSystem = new MemoryEntitlementSystem().create(perms, Collections.emptySet(), config);
//        EntitlementExpressionProvider entitlementExpressionProvider = entitlementSystem.getExpressionProvider();
        bind(EntitlementExpressionProvider.class).toProvider(Providers.<EntitlementExpressionProvider>of(null));

        bind(MiruReader.class).to(MiruReaderImpl.class);
        bind(MiruWriter.class).to(MiruWriterImpl.class);
    }

}
