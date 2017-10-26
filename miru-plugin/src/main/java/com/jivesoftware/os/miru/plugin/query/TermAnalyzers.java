package com.jivesoftware.os.miru.plugin.query;

import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.ckb.SoraniAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;

/**
 *
 */
public class TermAnalyzers {

    private static final CharArraySet EMPTY_STOP_WORDS_SET = new CharArraySet(Collections.emptyList(), false);

    private static final Analyzer STOPWORD_STANDARD_ANALYZER = new StandardAnalyzer(EnStopwords.ENGLISH_STOP_WORDS_SET);
    private static final Analyzer NON_STOPWORD_STANDARD_ANALYZER = new StandardAnalyzer(EMPTY_STOP_WORDS_SET);

    private final Map<String, Analyzer> stopwordAnalyzers = ImmutableMap.<String, Analyzer>builder()
        .put("ar", new ArabicAnalyzer())
        .put("bg", new BulgarianAnalyzer())
        .put("ca", new CatalanAnalyzer())
        .put("cs", new CzechAnalyzer())
        .put("en", new EnglishAnalyzer(EnStopwords.ENGLISH_STOP_WORDS_SET))
        .put("en_ie", new IrishAnalyzer(EnStopwords.ENGLISH_STOP_WORDS_SET))
        .put("es", new SpanishAnalyzer())
        .put("eu", new BasqueAnalyzer())
        .put("da", new DanishAnalyzer())
        .put("de", new GermanAnalyzer())
        .put("fa", new PersianAnalyzer())
        .put("fi", new FinnishAnalyzer())
        .put("fr", new FrenchAnalyzer())
        .put("gl", new GalicianAnalyzer())
        .put("gr", new GreekAnalyzer())
        .put("hi", new HindiAnalyzer())
        .put("hy", new ArmenianAnalyzer())
        .put("id", new IndonesianAnalyzer())
        .put("it", new ItalianAnalyzer())
        .put("ku", new SoraniAnalyzer())
        .put("lv", new LatvianAnalyzer())
        .put("nl", new DutchAnalyzer())
        .put("nb", new NorwegianAnalyzer())
        .put("nn", new NorwegianAnalyzer())
        .put("pt", new PortugueseAnalyzer())
        .put("pt_br", new BrazilianAnalyzer())
        .put("ro", new RomanianAnalyzer())
        .put("ru", new RussianAnalyzer())
        .put("sv", new SwedishAnalyzer())
        .put("th", new ThaiAnalyzer())
        .put("tr", new TurkishAnalyzer())
        .build();
    private final Map<String, Analyzer> nonStopwordAnalyzers = ImmutableMap.<String, Analyzer>builder()
        .put("ar", new ArabicAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("bg", new BulgarianAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("ca", new CatalanAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("cs", new CzechAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("en", new EnglishAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("en_ie", new IrishAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("es", new SpanishAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("eu", new BasqueAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("da", new DanishAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("de", new GermanAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("fa", new PersianAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("fi", new FinnishAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("fr", new FrenchAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("gl", new GalicianAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("gr", new GreekAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("hi", new HindiAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("hy", new ArmenianAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("id", new IndonesianAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("it", new ItalianAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("ku", new SoraniAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("lv", new LatvianAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("nl", new DutchAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("nb", new NorwegianAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("nn", new NorwegianAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("pt", new PortugueseAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("pt_br", new BrazilianAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("ro", new RomanianAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("ru", new RussianAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("sv", new SwedishAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("th", new ThaiAnalyzer(EMPTY_STOP_WORDS_SET))
        .put("tr", new TurkishAnalyzer(EMPTY_STOP_WORDS_SET))
        .build();

    public Analyzer findAnalyzer(String locale, boolean useStopWords) {
        Map<String, Analyzer> analyzers = useStopWords ? stopwordAnalyzers : nonStopwordAnalyzers;
        Analyzer analyzer = null;
        if (locale != null && !locale.isEmpty()) {
            String code = locale;
            analyzer = analyzers.get(code);
            while (analyzer == null) {
                int ix = code.indexOf('_');
                if (ix == -1) {
                    break;
                }
                code = code.substring(0, ix);
                analyzer = analyzers.get(code);
            }
        }
        if (analyzer == null) {
            analyzer = useStopWords ? STOPWORD_STANDARD_ANALYZER : NON_STOPWORD_STANDARD_ANALYZER;
        }
        return analyzer;
    }


    public static void main(String[] args) throws Exception {
        String locale = "en";
        boolean useStopWords = false;

        if (args.length > 0) {
            locale = args[0];
        }
        if (args.length > 1) {
            useStopWords = Boolean.valueOf(args[1]);
        }

        System.out.println("Build analyzer");
        TermAnalyzers termAnalyzers = new TermAnalyzers();
        System.out.println("Find analyzer");
        Analyzer analyzer = termAnalyzers.findAnalyzer(locale, useStopWords);
        System.out.println("Analyzer: " + analyzer);
    }

}
