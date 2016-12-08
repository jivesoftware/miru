package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import java.util.Collection;
import java.util.Map;

/**
 *
 */
public interface FullTextTermProvider {

    boolean isEnabled();

    Map<MiruValue, String[]> getText(Collection<MiruValue> values);
}
