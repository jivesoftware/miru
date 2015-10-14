package com.jivesoftware.os.miru.plugin.query;

import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import static org.apache.commons.lang.StringUtils.trimToNull;

/**
 *
 */
public class Tokenizer {

    private Analyzer analyzer;
    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public Tokenizer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public List<String> tokenize(String data) {
        return tokenize(data, 1);
    }

    private List<String> tokenize(String data, int maximumSize) {
        LOG.debug("tokenize data: {}, maximumSize: {}", data != null ? data.length() : -1, maximumSize);

        List<String> terms = new ArrayList<String>();

        try {
            TokenStream tokens = analyzer.tokenStream(null, new StringReader(data));

            if (maximumSize >= 2) {
                tokens = new ShingleFilter(tokens, maximumSize);
            }
            tokens = new StandardFilter(tokens);

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
            LOG.warn("unable to tokenize data. cause: {}" + ioe.getMessage(), ioe);
        }

        return maximumSize >= 2 ? pruneFiller(terms) : terms;
    }

    private List<String> pruneFiller(List<String> terms) {
        List<String> filteredTerms = new ArrayList<String>();

        for (String term : terms) {
            if (!term.contains("_")) {
                filteredTerms.add(term);
            }
        }

        return filteredTerms;
    }
}
