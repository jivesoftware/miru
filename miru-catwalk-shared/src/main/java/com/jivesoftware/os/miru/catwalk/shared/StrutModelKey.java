package com.jivesoftware.os.miru.catwalk.shared;

import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.Arrays;

/**
 *
 */
public class StrutModelKey {

    private final MiruTermId[] termIds;

    public StrutModelKey(MiruTermId[] termIds) {
        this.termIds = termIds;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 23 * hash + Arrays.deepHashCode(this.termIds);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final StrutModelKey other = (StrutModelKey) obj;
        if (!Arrays.deepEquals(this.termIds, other.termIds)) {
            return false;
        }
        return true;
    }

}
