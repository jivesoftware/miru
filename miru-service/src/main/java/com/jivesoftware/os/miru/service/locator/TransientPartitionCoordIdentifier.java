package com.jivesoftware.os.miru.service.locator;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;

/**
 *
 */
public class TransientPartitionCoordIdentifier implements MiruResourcePartitionIdentifier {

    private static final int NUM_HASH_DIRS = 1024;

    private final MiruPartitionCoord coord;

    public TransientPartitionCoordIdentifier(MiruPartitionCoord coord) {
        this.coord = coord;
    }

    @Override
    public String[] getParts() {
        String hashDir = "tenantHash-" + String.valueOf(Math.abs(coord.tenantId.hashCode() % NUM_HASH_DIRS));
        return new String[] { hashDir, coord.tenantId.toString(), coord.partitionId.toString() + ".tmp" };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TransientPartitionCoordIdentifier that = (TransientPartitionCoordIdentifier) o;

        return !(coord != null ? !coord.equals(that.coord) : that.coord != null);
    }

    @Override
    public int hashCode() {
        return coord != null ? coord.hashCode() : 0;
    }
}
