package com.jivesoftware.os.wiki.miru.deployable.topics;

import com.google.common.base.Joiner;
import com.google.common.collect.MinMaxPriorityQueue;
import com.jivesoftware.os.miru.plugin.query.TermTokenizer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;

/**
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
    public static Topic[] getKeywordsList(String fullText, int topN) throws IOException {

        Analyzer nonSteming = new NonStemingEnglishAnalyzer(EnStopwords.ENGLISH_STOP_WORDS_SET);

        Analyzer analyzer = new EnglishAnalyzer(EnStopwords.ENGLISH_STOP_WORDS_SET);

        TermTokenizer termTokenizer = new TermTokenizer();
        List<String> tokens = termTokenizer.tokenize(nonSteming, fullText);

        List<CardKeyword> cardKeywords = new LinkedList<>();

        //CharTermAttribute token = tokenStream.getAttribute(CharTermAttribute.class);

        List<String> terms = new ArrayList<>();
        List<String> stems = new ArrayList<>();

        for (String term : tokens) {

            List<String> stem = termTokenizer.tokenize(analyzer, term);
            if (stem != null && stem.size() == 1) {

                String s = stem.get(0);
                terms.add(term);
                stems.add(s);

                CardKeyword cardKeyword = find(cardKeywords, new CardKeyword(s.replaceAll("-0", "-")));
                // treat the dashed words back, let look them pretty
                cardKeyword.add(term.replaceAll("-0", "-"));
            }
        }

        MinMaxPriorityQueue<Topic> topics = MinMaxPriorityQueue.maximumSize(topN).expectedSize(topN).create();

        Map<String, Integer> stemFrequency = new HashMap<>();
        for (CardKeyword cardKeyword : cardKeywords) {
            System.out.println(cardKeyword);
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
                int k = 0;
                for (String s : stems.subList(t, t + l + 1)) {
                    if (j.add(s)) {
                        ts.add(terms.get(t + k));
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