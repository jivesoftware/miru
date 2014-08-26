package com.jivesoftware.os.miru.service.stream.locator;

/**
 *
 */
public class MiruHybridTokenIdentifier implements MiruResourcePartitionIdentifier {

    private final String token;

    public MiruHybridTokenIdentifier(String token) {
        this.token = token;
    }

    @Override
    public String[] getParts() {
        return new String[] { token };
    }

    public String getToken() {
        return token;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MiruHybridTokenIdentifier that = (MiruHybridTokenIdentifier) o;

        if (token != null ? !token.equals(that.token) : that.token != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return token != null ? token.hashCode() : 0;
    }
}
