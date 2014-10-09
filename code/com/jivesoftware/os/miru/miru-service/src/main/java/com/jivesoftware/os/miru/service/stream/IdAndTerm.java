package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.miru.api.base.MiruTermId;

/**
*
*/
class IdAndTerm {
    final int id;
    final MiruTermId term;

    IdAndTerm(int id, MiruTermId term) {
        this.id = id;
        this.term = term;
    }

    @Override
    public String toString() {
        return "{" + id + ',' + term + '}';
    }
}
