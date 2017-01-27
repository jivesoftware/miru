package com.jivesoftware.os.wiki.miru.deployable.topics;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Multiset;
import com.jivesoftware.os.miru.plugin.query.EnStopwords;
import com.jivesoftware.os.miru.plugin.query.TermTokenizer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

/**
 * <p>
 * Keywords extractor functionality handler
 */
public class KeywordsExtractor {

    public static void main(String[] args) throws IOException {


        String text = "cat cat cat cat cat cat cat frog mouse frog mouse frog mouse frog mouse frog mouse";

        Topic[] topics = KeywordsExtractor.getKeywordsList(text, 20, 20);

        for (Topic topic : topics) {
            System.out.println(topic.toString());
        }

        String image = findImage(Joiner.on("+").join(topics[0].topic), null);
        System.out.println(image);

    }

    public static String findImage(String question, String ua) {
        ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36";
        String finRes = "";

        try {
            String googleUrl = "https://www.google.com/search?tbm=isch&q=" + question.replace(",", "");
            Document doc1 = Jsoup.connect(googleUrl).userAgent(ua).timeout(10 * 1000).get();
            Element media = doc1.select("[data-src]").first();
            String finUrl = media.attr("abs:data-src");

            finRes = finUrl.replace("&quot", "");

        } catch (Exception e) {
            System.out.println(e);
        }

        return finRes;
    }

    /**
     * Get list of keywords with stem form, frequency rank, and terms dictionary
     *
     * @param fullText
     * @return List<CardKeyword>, which contains keywords cards
     * @throws IOException
     */
    public static Topic[] getKeywordsList(String fullText, int maxPhraseLength, int topN) throws IOException {

        Analyzer analyzer = new EnglishAnalyzer(EnStopwords.ENGLISH_STOP_WORDS_SET);
        Analyzer nonSteming = new NonStemingEnglishAnalyzer(EnStopwords.ENGLISH_STOP_WORDS_SET);

        TermTokenizer termTokenizer = new TermTokenizer();
        List<String> tokens = termTokenizer.tokenize(nonSteming, fullText);
        List<String> stemmedTokens = termTokenizer.tokenize(analyzer, fullText);

        Map<String, Keyword> keywords = new HashMap<>();
        List<String> terms = new ArrayList<>();
        List<String> stems = new ArrayList<>();

        int z = 0;
        for (String term : tokens) {

            String s = stemmedTokens.get(z);
            Keyword keyword = keywords.computeIfAbsent(s, s1 -> {
                return new Keyword(term);
            });
            keyword.add(term);

            terms.add(term);
            stems.add(s);

            z++;
        }

        MinMaxPriorityQueue<Topic> topics = MinMaxPriorityQueue.maximumSize(topN).expectedSize(topN).create();

        String[] empty = new String[0];

        Set<String> uniq = new HashSet<>();
        int m = maxPhraseLength;
        for (int t = 0; t < terms.size(); t++) {

            int score = 1;
            for (int i = t, l = 0; i < terms.size() && l < m; i++, l++) {
                score += keywords.get(stems.get(i)).frequency;
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


    static class Keyword implements Comparable<Keyword> {

        final String stem;

        final Multiset<String> terms = HashMultiset.create();

        int frequency;

        Keyword(String stem) {
            this.stem = stem;
        }

        public void add(String term) {
            this.terms.add(term);
            this.frequency++;
        }

        @Override
        public int compareTo(Keyword keyword) {
            return Integer.compare(keyword.frequency, this.frequency);
        }


        @Override
        public String toString() {
            return "CardKeyword{" +
                "stem='" + stem + '\'' +
                ", terms=" + terms +
                ", frequency=" + frequency +
                '}';
        }
    }
}