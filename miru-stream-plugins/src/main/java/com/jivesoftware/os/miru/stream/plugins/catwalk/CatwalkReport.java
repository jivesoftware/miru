package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.Serializable;

/**
 * @author jonathan
 */
public class CatwalkReport implements Serializable {

    @JsonCreator
    public CatwalkReport() {
    }

    @Override
    public String toString() {
        return "CatwalkReport{}";
    }
}
