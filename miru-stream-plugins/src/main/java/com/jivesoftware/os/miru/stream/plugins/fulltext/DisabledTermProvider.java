package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import java.util.Collection;
import java.util.Map;

/**
 *
 */
public class DisabledTermProvider implements FullTextTermProvider {

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public Map<MiruValue, String[]> getText(Collection<MiruValue> values) {
        throw new UnsupportedOperationException("Please configure a term provider");
    }
}
