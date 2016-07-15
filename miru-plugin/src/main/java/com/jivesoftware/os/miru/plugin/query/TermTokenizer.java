package com.jivesoftware.os.miru.plugin.query;

import com.google.common.collect.Lists;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import static org.apache.commons.lang.StringUtils.trimToNull;

/**
 *
 */
public class TermTokenizer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public List<String> tokenize(Analyzer analyzer, String data) {
        List<String> terms = Lists.newArrayList();
        try {
            TokenStream tokens = new StandardFilter(analyzer.tokenStream(null, new StringReader(data)));
            tokens.reset();

            while (tokens.incrementToken()) {
                CharTermAttribute termAttribute = tokens.getAttribute(CharTermAttribute.class);
                String term = trimToNull(termAttribute.toString());
                if (term != null) {
                    terms.add(term);
                }
            }

            tokens.end();
            tokens.close();
        } catch (IOException ioe) {
            LOG.warn("Unable to tokenize data. cause: {}", new Object[] { ioe.getMessage() }, ioe);
        }
        return terms;
    }
}
