package com.jivesoftware.os.miru.reco.plugins.trending;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.base.MiruTermId;

/**
 *
 */
class TrendingWaveformKey {

    final MiruPartitionCoord coord;
    final int fieldId;
    final long ceilingTime;
    final long floorTime;
    final int numSegments;
    final MiruTermId termId;

    TrendingWaveformKey(MiruPartitionCoord coord, int fieldId, long ceilingTime, long floorTime, int numSegments, MiruTermId termId) {
        this.coord = coord;
        this.fieldId = fieldId;
        this.ceilingTime = ceilingTime;
        this.floorTime = floorTime;
        this.numSegments = numSegments;
        this.termId = termId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TrendingWaveformKey that = (TrendingWaveformKey) o;

        if (fieldId != that.fieldId) {
            return false;
        }
        if (ceilingTime != that.ceilingTime) {
            return false;
        }
        if (floorTime != that.floorTime) {
            return false;
        }
        if (numSegments != that.numSegments) {
            return false;
        }
        if (coord != null ? !coord.equals(that.coord) : that.coord != null) {
            return false;
        }
        return !(termId != null ? !termId.equals(that.termId) : that.termId != null);

    }

    @Override
    public int hashCode() {
        int result = coord != null ? coord.hashCode() : 0;
        result = 31 * result + fieldId;
        result = 31 * result + (int) (ceilingTime ^ (ceilingTime >>> 32));
        result = 31 * result + (int) (floorTime ^ (floorTime >>> 32));
        result = 31 * result + numSegments;
        result = 31 * result + (termId != null ? termId.hashCode() : 0);
        return result;
    }
}
