package com.jivesoftware.os.miru.service.index.auth;

/**
*
*/
public class VersionedAuthz {

    public final String authz;
    public final long version;

    private int hashCode = 0;

    public VersionedAuthz(String authz, long version) {
        this.authz = authz;
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        VersionedAuthz that = (VersionedAuthz) o;

        if (version != that.version) {
            return false;
        }
        if (authz != null ? !authz.equals(that.authz) : that.authz != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = computeHashCode();
        }
        return hashCode;
    }

    private int computeHashCode() {
        int result = authz != null ? authz.hashCode() : 0;
        result = 31 * result + (int) (version ^ (version >>> 32));
        return result;
    }
}
