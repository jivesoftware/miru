package com.jivesoftware.os.wiki.miru.deployable.topics;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * Created by jonathan.colt on 11/16/16.
 */
public class NonStemingEnglishAnalyzer extends StopwordAnalyzerBase {
    private final CharArraySet stemExclusionSet;


    /**
     * Builds an analyzer with the given stop words. If a non-empty stem exclusion set is
     * provided this analyzer will add a {@link SetKeywordMarkerFilter} before
     * stemming.
     *
     * @param stopwords a stopword set
     */
    public NonStemingEnglishAnalyzer(CharArraySet stopwords) {
        super(stopwords);
        this.stemExclusionSet = new CharArraySet(10, true) {
            public boolean contains(char[] text, int off, int len) {
                return true;
            }

            public boolean contains(CharSequence cs) {
                return true;
            }

            @Override
            public boolean contains(Object o) {
                return true;
            }

            public int size() {
                return 1;
            }


        };
    }


    /**
     * Creates a
     * {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
     * which tokenizes all the text in the provided {@link java.io.Reader}.
     *
     * @return A
     * {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
     * built from an {@link StandardTokenizer} filtered with
     * {@link StandardFilter}, {@link EnglishPossessiveFilter},
     * {@link LowerCaseFilter}, {@link StopFilter}
     * , {@link SetKeywordMarkerFilter} if a stem exclusion set is
     * provided and {@link PorterStemFilter}.
     */
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final Tokenizer source = new StandardTokenizer();
        TokenStream result = new StandardFilter(source);
        // prior to this we get the classic behavior, standardfilter does it for us.
        result = new EnglishPossessiveFilter(result);
        result = new LowerCaseFilter(result);
        result = new StopFilter(result, stopwords);
        if (!stemExclusionSet.isEmpty()) {
            result = new SetKeywordMarkerFilter(result, stemExclusionSet);
        }
        result = new PorterStemFilter(result);
        return new TokenStreamComponents(source, result);
    }

}