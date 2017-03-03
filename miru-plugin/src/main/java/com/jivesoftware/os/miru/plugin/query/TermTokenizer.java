package com.jivesoftware.os.miru.plugin.query;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
import com.google.common.collect.Sets;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;

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

    private static final boolean average = true;

    private static float score(float... scores) {
        if (average) {
            float s = 0;
            for (float score : scores) {
                s += score;
            }
            return s / scores.length;
        } else {
            float s = 1f;
            for (float score : scores) {
                s *= score;
            }
            return s;
        }
    }

    public static void main(String[] args) throws Exception {
        String rootContent = "";

        String base = "Test test test";
        String[] split = base.replace("'", "").split("\\p{Punct}");

        List<String> stopwords = Arrays.asList("the",
            "of", "to", "and", "a", "in", "is", "it", "its", "you", "that", "he", "was", "for", "on", "are", "with", "as", "I", "his", "they", "be", "at",
            "one", "than", "have", "this", "from", "or", "had", "by", "hot", "word", "but", "what", "some", "we", "can", "out", "other", "were", "all",
            "there", "when", "up", "use", "your", "how", "said", "an", "each", "she", "which", "do", "their", "time", "if", "will", "way", "about", "any",
            "many", "then", "them", "would", "like", "so", "these", "her", "long", "make", "thing", "see", "him", "two", "has", "our", "not", "doesn't",
            "per");
        //TermAnalyzers termAnalyzers = new TermAnalyzers();
        Analyzer analyzer = new EnglishAnalyzer(new CharArraySet(stopwords, true)); //termAnalyzers.findAnalyzer("en");
        //CharArraySet stopwordSet = ((StopwordAnalyzerBase) analyzer).getStopwordSet();

        TermTokenizer termTokenizer = new TermTokenizer();
        List<String> tokens = termTokenizer.tokenize(analyzer, rootContent + " " + base);

        Multiset<String> counted = HashMultiset.create();
        counted.addAll(tokens);

        float avg = 0f;
        for (Entry<String> entry : counted.entrySet()) {
            avg += entry.getCount();
        }
        avg /= counted.elementSet().size();

        Set<String> already = Sets.newHashSet();
        List<GramScore> ngrams = Lists.newArrayList();
        for (int j = 0; j < split.length; j++) {
            tokens = termTokenizer.tokenize(analyzer, split[j]);

            // 1-grams
            for (int i = 0; i < tokens.size() - 1; i++) {
                float score1 = counted.count(tokens.get(i)) / avg;
                String gram = tokens.get(i);
                if (already.add(gram)) {
                    ngrams.add(new GramScore(gram, score(score1)));
                }
            }
            // 2-grams
            for (int i = 0; i < tokens.size() - 1; i++) {
                float score1 = counted.count(tokens.get(i)) / avg;
                float score2 = counted.count(tokens.get(i + 1)) / avg;
                SortedSet<String> grams = Sets.newTreeSet();
                grams.add(tokens.get(i));
                grams.add(tokens.get(i + 1));
                if (grams.size() == 2) {
                    String gram = Joiner.on(" ").join(grams);
                    if (already.add(gram)) {
                        ngrams.add(new GramScore(gram, score(score1, score2)));
                    }
                }
            }
            // 3-grams
            for (int i = 0; i < tokens.size() - 2; i++) {
                float score1 = counted.count(tokens.get(i)) / avg;
                float score2 = counted.count(tokens.get(i + 1)) / avg;
                float score3 = counted.count(tokens.get(i + 2)) / avg;
                SortedSet<String> grams = Sets.newTreeSet();
                grams.add(tokens.get(i));
                grams.add(tokens.get(i + 1));
                grams.add(tokens.get(i + 2));
                if (grams.size() == 3) {
                    String gram = Joiner.on(" ").join(grams);
                    if (already.add(gram)) {
                        ngrams.add(new GramScore(gram, score(score1, score2, score3)));
                    }
                }
            }
        }

        Collections.sort(ngrams, (o1, o2) -> Float.compare(o2.score, o1.score));

        System.out.println("avg: " + avg);
        for (int i = 0; i < Math.min(ngrams.size(), 20); i++) {
            GramScore ngram = ngrams.get(i);
            System.out.println(ngram);
        }
    }

    private static class GramScore {
        private final String gram;
        private final float score;

        private GramScore(String gram, float score) {
            this.gram = gram;
            this.score = score;
        }

        @Override
        public String toString() {
            return gram + " -> " + score;
        }
    }
}
