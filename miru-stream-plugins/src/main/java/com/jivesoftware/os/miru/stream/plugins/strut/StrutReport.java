package com.jivesoftware.os.miru.stream.plugins.strut;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.Serializable;

/**
 * @author jonathan
 */
public class StrutReport implements Serializable {


    @JsonCreator
    public StrutReport() {
    }

    @Override
    public String toString() {
        return "StrutReport{" +
            '}';
    }
}
