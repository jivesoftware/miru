package com.jivesoftware.os.miru.plugin.query;

import com.jivesoftware.os.miru.api.query.filter.MiruFilter;

/**
 *
 */
public interface MiruQueryParser {

    MiruFilter parse(String locale, boolean useStopWords, String query) throws Exception;

    String highlight(String locale, boolean useStopWords, String query, String content, String pre, String post, int preview);
}
