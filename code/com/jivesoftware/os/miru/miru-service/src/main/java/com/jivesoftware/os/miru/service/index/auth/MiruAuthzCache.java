package com.jivesoftware.os.miru.service.index.auth;

import com.google.common.cache.Cache;
import com.google.common.collect.Interner;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class MiruAuthzCache {

    private final Cache<VersionedAuthzExpression, EWAHCompressedBitmap> cache;
    private final ConcurrentMap<String, VersionableAuthz> versionables = Maps.newConcurrentMap();
    private final Interner<String> interner;
    private final MiruAuthzUtils utils;

    public MiruAuthzCache(Cache<VersionedAuthzExpression, EWAHCompressedBitmap> cache, Interner<String> interner, MiruAuthzUtils utils) {
        this.cache = cache;
        this.interner = interner;
        this.utils = utils;
    }

    public long sizeInBytes() {
        long sizeInBytes = 0;
        for (Map.Entry<VersionedAuthzExpression, EWAHCompressedBitmap> entry : cache.asMap().entrySet()) {
            sizeInBytes += entry.getKey().sizeInBytes() + entry.getValue().sizeInBytes();
        }
        for (String key : versionables.keySet()) {
            sizeInBytes += key.length() * 2;
        }
        sizeInBytes += versionables.size() * 16; // reference, long
        return sizeInBytes;
    }

    public void increment(String authz) {
        currentVersion(authz).increment();
    }

    public EWAHCompressedBitmap getOrCompose(MiruAuthzExpression authzExpression, MiruAuthzUtils.IndexRetriever indexRetriever) throws Exception {
        VersionedAuthzExpression key = new VersionedAuthzExpression(currentVersions(authzExpression));
        EWAHCompressedBitmap got = cache.getIfPresent(key);
        if (got == null) {
            got = utils.getCompositeAuthz(authzExpression, indexRetriever);
            cache.put(key, got);
        }
        return got;
    }

    public void clear() {
        cache.invalidateAll();
        versionables.clear();
    }

    private Set<VersionedAuthz> currentVersions(MiruAuthzExpression authzExpression) {
        Set<VersionedAuthz> versions = Sets.newHashSet();
        for (String authz : authzExpression.values) {
            versions.add(currentVersion(interner.intern(authz)).getLatest());
        }
        return versions;
    }

    private VersionableAuthz currentVersion(String authz) {
        VersionableAuthz versionableAuthz = versionables.get(authz);
        if (versionableAuthz == null) {
            versionableAuthz = new VersionableAuthz(authz);
            VersionableAuthz existing = versionables.putIfAbsent(authz, versionableAuthz);
            if (existing != null) {
                versionableAuthz = existing;
            }
        }
        return versionableAuthz;
    }

    private static class VersionableAuthz {

        private final String authz;
        private final AtomicLong version = new AtomicLong();

        private VersionedAuthz versionedAuthz;

        private VersionableAuthz(String authz) {
            this.authz = authz;
        }

        synchronized private void increment() {
            version.incrementAndGet();
            versionedAuthz = null;
        }

        synchronized private VersionedAuthz getLatest() {
            if (versionedAuthz == null) {
                versionedAuthz = new VersionedAuthz(authz, version.get());
            }
            return versionedAuthz;
        }
    }

}
