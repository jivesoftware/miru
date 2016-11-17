package com.jivesoftware.os.miru.plugin.query;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
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

    private final Analyzer STANDARD_ANALYZER = new StandardAnalyzer();
    private final Map<String, Analyzer> analyzers = ImmutableMap.<String, Analyzer>builder()
        .put("ar", new ArabicAnalyzer())
        .put("bg", new BulgarianAnalyzer())
        .put("ca", new CatalanAnalyzer())
        .put("cs", new CzechAnalyzer())
        .put("en", new EnglishAnalyzer())
        .put("en_ie", new IrishAnalyzer())
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

    public Analyzer findAnalyzer(String locale) {
        Analyzer analyzer = null;
        if (locale != null && !locale.isEmpty()) {
            String code = locale;
            analyzer = analyzers.get(code);
            while (analyzer == null) {
                int ix = locale.indexOf('_');
                if (ix == -1) {
                    break;
                }
                code = code.substring(0, ix);
                analyzer = analyzers.get(code);
            }
        }
        if (analyzer == null) {
            analyzer = STANDARD_ANALYZER;
        }
        return analyzer;
    }
}
