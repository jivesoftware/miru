package com.jivesoftware.os.miru.stream.plugins.strut;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery.CatwalkModelQuery;
import java.io.Serializable;

/**
 *
 */
public class CatwalkScorable implements Serializable {

    public final CatwalkModelQuery modelQuery;
    public final boolean shareRemote;

    @JsonCreator
    public CatwalkScorable(@JsonProperty("modelQuery") CatwalkModelQuery modelQuery,
        @JsonProperty("shareRemote") boolean shareRemote) {
        this.modelQuery = modelQuery;
        this.shareRemote = shareRemote;
    }

    @Override
    public String toString() {
        return "CatwalkScorable{" +
            "modelQuery=" + modelQuery +
            ", shareRemote=" + shareRemote +
            '}';
    }
}
