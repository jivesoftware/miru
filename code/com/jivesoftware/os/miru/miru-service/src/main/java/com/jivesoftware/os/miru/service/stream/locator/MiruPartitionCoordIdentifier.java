package com.jivesoftware.os.miru.service.stream.locator;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;

/**
 *
 */
public class MiruPartitionCoordIdentifier implements MiruResourcePartitionIdentifier {

    private final MiruPartitionCoord coord;

    public MiruPartitionCoordIdentifier(MiruPartitionCoord coord) {
        this.coord = coord;
    }

    @Override
    public String[] getParts() {
        return new String[] { coord.tenantId.toString(), coord.partitionId.toString() };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MiruPartitionCoordIdentifier that = (MiruPartitionCoordIdentifier) o;

        if (coord != null ? !coord.equals(that.coord) : that.coord != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return coord != null ? coord.hashCode() : 0;
    }
}
