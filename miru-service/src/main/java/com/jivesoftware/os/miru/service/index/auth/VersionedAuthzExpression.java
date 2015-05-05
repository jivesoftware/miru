package com.jivesoftware.os.miru.service.index.auth;

import java.util.Set;

/**
*
*/
public class VersionedAuthzExpression {

    private final Set<VersionedAuthz> versions;

    public VersionedAuthzExpression(Set<VersionedAuthz> versions) {
        this.versions = versions;
    }

    public long sizeInBytes() {
        long size = 0;
        for (VersionedAuthz version : versions) {
            size += version.authz.length() * 2 + 8; // 2 bytes per char plus long version
        }
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        VersionedAuthzExpression that = (VersionedAuthzExpression) o;

        return !(versions != null ? !versions.equals(that.versions) : that.versions != null);
    }

    @Override
    public int hashCode() {
        return versions != null ? versions.hashCode() : 0;
    }
}
