package com.jivesoftware.os.miru.api.plugin;

/**
*
*/
public class MiruEndpointInjectable<T> {

    private final Class<T> injectableClass;
    private final T injectable;

    public MiruEndpointInjectable(Class<T> injectableClass, T injectable) {
        this.injectableClass = injectableClass;
        this.injectable = injectable;
    }

    public Class<T> getInjectableClass() {
        return injectableClass;
    }

    public T getInjectable() {
        return injectable;
    }
}
