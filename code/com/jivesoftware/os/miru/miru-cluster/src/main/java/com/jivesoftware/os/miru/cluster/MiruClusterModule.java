package com.jivesoftware.os.miru.cluster;

import com.google.inject.AbstractModule;

public class MiruClusterModule extends AbstractModule {

    private final Class<? extends MiruHostsStorage> hostsStorageClass;
    private final Class<? extends MiruHostsPartitionsStorage> hostsPartitionsStorageClass;

    public MiruClusterModule(
        Class<? extends MiruHostsStorage> hostsStorageClass,
        Class<? extends MiruHostsPartitionsStorage> hostsPartitionsStorageClass) {
        this.hostsStorageClass = hostsStorageClass;
        this.hostsPartitionsStorageClass = hostsPartitionsStorageClass;
    }

    @Override
    protected void configure() {
        bind(MiruHostsStorage.class).to(hostsStorageClass);
        bind(MiruHostsPartitionsStorage.class).to(hostsPartitionsStorageClass);
    }
}
