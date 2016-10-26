package com.jivesoftware.os.wiki.miru.deployable;

import com.google.common.collect.ComparisonChain;

/**
 *
 */
public class ServiceId implements Comparable<ServiceId> {

    public final String datacenter;
    public final String cluster;
    public final String host;
    public final String service;
    public final String instance;
    public final String version;

    public ServiceId(String datacenter, String cluster, String host, String service, String instance, String version) {
        this.datacenter = datacenter;
        this.cluster = cluster;
        this.host = host;
        this.service = service;
        this.instance = instance;
        this.version = version;
    }

    @Override
    public int compareTo(ServiceId o) {
        return ComparisonChain.start()
            .compare(datacenter, o.datacenter)
            .compare(cluster, o.cluster)
            .compare(host, o.host)
            .compare(service, o.service)
            .compare(instance, o.instance)
            .compare(version, o.version)
            .result();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ServiceId serviceId = (ServiceId) o;

        if (cluster != null ? !cluster.equals(serviceId.cluster) : serviceId.cluster != null) {
            return false;
        }
        if (datacenter != null ? !datacenter.equals(serviceId.datacenter) : serviceId.datacenter != null) {
            return false;
        }
        if (host != null ? !host.equals(serviceId.host) : serviceId.host != null) {
            return false;
        }
        if (instance != null ? !instance.equals(serviceId.instance) : serviceId.instance != null) {
            return false;
        }
        if (service != null ? !service.equals(serviceId.service) : serviceId.service != null) {
            return false;
        }
        if (version != null ? !version.equals(serviceId.version) : serviceId.version != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = datacenter != null ? datacenter.hashCode() : 0;
        result = 31 * result + (cluster != null ? cluster.hashCode() : 0);
        result = 31 * result + (host != null ? host.hashCode() : 0);
        result = 31 * result + (service != null ? service.hashCode() : 0);
        result = 31 * result + (instance != null ? instance.hashCode() : 0);
        result = 31 * result + (version != null ? version.hashCode() : 0);
        return result;
    }
}
