package com.jivesoftware.os.miru.service.index.auth;

import com.google.common.cache.Cache;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.service.stream.MiruActivityInternExtern;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class MiruAuthzCache<BM> {

    private final MiruBitmaps<BM> bitmaps;
    private final Cache<VersionedAuthzExpression, BM> cache;
    private final ConcurrentMap<String, VersionableAuthz> versionables = Maps.newConcurrentMap();
    private final MiruActivityInternExtern interner;
    private final MiruAuthzUtils<BM> utils;

    public MiruAuthzCache(MiruBitmaps<BM> bitmaps, Cache<VersionedAuthzExpression, BM> cache, MiruActivityInternExtern interner, MiruAuthzUtils<BM> utils) {
        this.bitmaps = bitmaps;
        this.cache = cache;
        this.interner = interner;
        this.utils = utils;
    }

    public long sizeInBytes() {
        long sizeInBytes = 0;
        for (Map.Entry<VersionedAuthzExpression, BM> entry : cache.asMap().entrySet()) {
            sizeInBytes += entry.getKey().sizeInBytes() + bitmaps.sizeInBytes(entry.getValue());
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

    public BM getOrCompose(MiruAuthzExpression authzExpression, MiruAuthzUtils.IndexRetriever<BM> indexRetriever) throws Exception {
        VersionedAuthzExpression key = new VersionedAuthzExpression(currentVersions(authzExpression));
        BM got = cache.getIfPresent(key);
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
            versions.add(currentVersion(interner.internString(authz)).getLatest());
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
