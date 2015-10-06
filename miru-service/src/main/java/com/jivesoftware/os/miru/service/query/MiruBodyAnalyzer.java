package com.jivesoftware.os.miru.service.query;

import java.io.Reader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 *
 */
public class MiruBodyAnalyzer extends Analyzer {

    protected TokenFilter applyExtendedFiltering(TokenStream tokenStream) {
        // Default do nothing
        return (TokenFilter) tokenStream;
    }

    @Override
    protected Analyzer.TokenStreamComponents createComponents(String fieldName, Reader reader) {
        StandardTokenizer standardTokenizer = new StandardTokenizer(reader);

        TokenFilter tok = new StandardFilter(standardTokenizer);
        tok = new LowerCaseFilter(tok);
        tok = new StopFilter(tok, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        tok = applyExtendedFiltering(tok);

        return new Analyzer.TokenStreamComponents(standardTokenizer, tok);
    }
}
