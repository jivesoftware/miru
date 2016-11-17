package com.jivesoftware.os.wiki.miru.deployable.topics;

import com.google.common.base.Joiner;
import com.google.common.collect.MinMaxPriorityQueue;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.ClassicFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * http://stackoverflow.com/questions/17447045/java-library-for-keywords-extraction-from-input-text
 * <p>
 * Keywords extractor functionality handler
 */
public class KeywordsExtractor {

    public static void main(String[] args) throws IOException {
        String text = "";


        Topic[] topics = KeywordsExtractor.getKeywordsList(text, 10);

        for (Topic topic : topics) {
            System.out.println(topic.toString());
        }

    }

    /**
     * Get list of keywords with stem form, frequency rank, and terms dictionary
     *
     * @param fullText
     * @return List<CardKeyword>, which contains keywords cards
     * @throws IOException
     */
    public static Topic[] getKeywordsList(String fullText,int topN) throws IOException {

        TokenStream tokenStream = null;

        try {
            // treat the dashed words, don't let separate them during the processing
            //fullText = fullText.replaceAll("-+", "-0");

            // replace any punctuation char but apostrophes and dashes with a space
            fullText = fullText.replaceAll("[\\p{Punct}&&[^'-]]+", " ");

            // replace most common English contractions
            fullText = fullText.replaceAll("(?:'(?:[tdsm]|[vr]e|ll))+\\b", "");

            StandardTokenizer stdToken = new StandardTokenizer(new StringReader(fullText));

            tokenStream = new StopFilter(new ASCIIFoldingFilter(new ClassicFilter(new LowerCaseFilter(stdToken))), EnStopwords.ENGLISH_STOP_WORDS_SET);
            tokenStream.reset();

            List<CardKeyword> cardKeywords = new LinkedList<>();

            CharTermAttribute token = tokenStream.getAttribute(CharTermAttribute.class);

            List<String> terms = new ArrayList<>();
            List<String> stems = new ArrayList<>();

            while (tokenStream.incrementToken()) {

                String term = token.toString();
                String stem = getStemForm(term);
                if (stem != null) {

                    terms.add(term);
                    stems.add(stem);

                    CardKeyword cardKeyword = find(cardKeywords, new CardKeyword(stem.replaceAll("-0", "-")));
                    // treat the dashed words back, let look them pretty
                    cardKeyword.add(term.replaceAll("-0", "-"));
                }
            }

            MinMaxPriorityQueue<Topic> topics = MinMaxPriorityQueue.maximumSize(topN).expectedSize(topN).create();

            Map<String, Integer> stemFrequency = new HashMap<>();
            for (CardKeyword cardKeyword : cardKeywords) {
                stemFrequency.put(cardKeyword.getStem(), cardKeyword.getFrequency());
            }

            String[] empty = new String[0];

            Set<String> uniq = new HashSet<>();
            int m = 4;
            for (int t = 0; t < terms.size(); t++) {

                int score = 1;
                for (int i = t, l = 0; i < terms.size() && l < m; i++, l++) {
                    score += stemFrequency.get(stems.get(i));
                    Set<String> j = new HashSet<>();
                    List<String> ts = new ArrayList<>();
                    int k=0;
                    for (String s : stems.subList(t, t + l + 1)) {
                        if (j.add(s)) {
                            ts.add(terms.get(t+k));
                        }
                        k++;
                    }
                    String u = Joiner.on(' ').join(j);
                    if (uniq.add(u)) {
                        topics.add(new Topic(ts.toArray(empty), score / (1f + l)));
                    }
                }
            }

            // reverse sort by frequency
            Topic[] finalTopics = topics.toArray(new Topic[0]);
            Arrays.sort(finalTopics);

            return finalTopics;
        } finally {
            if (tokenStream != null) {
                try {
                    tokenStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static public class Topic implements Comparable<Topic> {
        public final String[] topic;
        public final float score;

        public Topic(String[] topic, float score) {
            this.topic = topic;
            this.score = score;
        }

        @Override
        public int compareTo(Topic o) {
            return Float.compare(o.score, score);
        }

        @Override
        public String toString() {
            return Arrays.toString(topic) + "=" + score;
        }
    }

    /**
     * Get stem form of the term
     *
     * @param term
     * @return String, which contains the stemmed form of the term
     * @throws IOException
     */
    private static String getStemForm(String term) throws IOException {

        TokenStream tokenStream = null;

        try {
            StandardTokenizer stdToken = new StandardTokenizer(new StringReader(term));

            tokenStream = new PorterStemFilter(stdToken);
            tokenStream.reset();

            // eliminate duplicate tokens by adding them to a set
            Set<String> stems = new HashSet<>();

            CharTermAttribute token = tokenStream.getAttribute(CharTermAttribute.class);

            while (tokenStream.incrementToken()) {
                stems.add(token.toString());
            }

            // if stem form was not found or more than 2 stems have been found, return null
            if (stems.size() != 1) {
                return null;
            }

            String stem = stems.iterator().next();

            // if the stem form has non-alphanumerical chars, return null
            if (!stem.matches("[a-zA-Z0-9-]+")) {
                return null;
            }

            return stem;
        } finally {
            if (tokenStream != null) {
                try {
                    tokenStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Find sample in collection
     *
     * @param collection
     * @param sample
     * @param <T>
     * @return <T> T, which contains the found object within collection if exists, otherwise the initially searched object
     */
    private static <T> T find(Collection<T> collection, T sample) {

        for (T element : collection) {
            if (element.equals(sample)) {
                return element;
            }
        }

        collection.add(sample);

        return sample;
    }
}