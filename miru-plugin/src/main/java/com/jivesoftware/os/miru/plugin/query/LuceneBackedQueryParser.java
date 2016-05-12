package com.jivesoftware.os.miru.plugin.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import java.util.Collections;
import java.util.List;
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
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/**
 * Due to its reliance on the Lucene {@link QueryParser}, this class is NOT thread-safe.
 */
public class LuceneBackedQueryParser implements MiruQueryParser {

    private final String defaultField;
    //TODO it would be lovely if these were passed in
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

    public LuceneBackedQueryParser(String defaultField) {
        this.defaultField = defaultField;
    }

    @Override
    public MiruFilter parse(String locale, String queryString) throws Exception {
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
        QueryParser parser = new QueryParser(defaultField, analyzer);
        return makeFilter(parser.parse(queryString));
    }

    private MiruFilter makeFilter(Query query) {
        if (query instanceof BooleanQuery) {
            BooleanQuery bq = (BooleanQuery) query;
            List<MiruFilter> musts = Lists.newArrayList();
            List<MiruFilter> shoulds = Lists.newArrayList();
            List<MiruFilter> mustNots = Lists.newArrayList();
            for (BooleanClause clause : bq) {
                Query subQuery = clause.getQuery();
                MiruFilter subFilter = makeFilter(subQuery);
                if (clause.getOccur() == BooleanClause.Occur.MUST) {
                    musts.add(subFilter);
                } else if (clause.getOccur() == BooleanClause.Occur.SHOULD) {
                    shoulds.add(subFilter);
                } else if (clause.getOccur() == BooleanClause.Occur.MUST_NOT) {
                    mustNots.add(subFilter);
                }
            }
            return wrap(musts, shoulds, mustNots);
        } else if (query instanceof TermQuery) {
            TermQuery tq = (TermQuery) query;
            Term term = tq.getTerm();
            return new MiruFilter(MiruFilterOperation.and, false,
                Collections.singletonList(MiruFieldFilter.ofTerms(MiruFieldType.primary, term.field(), term.text())),
                Collections.<MiruFilter>emptyList());
        } else if (query instanceof PrefixQuery) {
            PrefixQuery pq = (PrefixQuery) query;
            Term term = pq.getPrefix();
            return new MiruFilter(MiruFilterOperation.and, false,
                Collections.singletonList(MiruFieldFilter.ofValues(MiruFieldType.primary, term.field(), new MiruValue(term.text(), "*"))),
                Collections.<MiruFilter>emptyList());
        } else {
            throw new IllegalArgumentException("Unsupported query type: " + query.getClass());
        }
    }

    private MiruFilter wrap(List<MiruFilter> musts, List<MiruFilter> shoulds, List<MiruFilter> mustNots) {
        if (!musts.isEmpty()) {
            if (!mustNots.isEmpty()) {
                List<MiruFilter> filters = Lists.newArrayList();
                filters.add(wrap(musts, shoulds, Collections.<MiruFilter>emptyList()));
                filters.addAll(mustNots);
                return new MiruFilter(MiruFilterOperation.pButNotQ, false,
                    Collections.<MiruFieldFilter>emptyList(),
                    filters);
            } else if (!shoulds.isEmpty()) {
                musts.add(wrap(Collections.<MiruFilter>emptyList(), shoulds, Collections.<MiruFilter>emptyList()));
                return new MiruFilter(MiruFilterOperation.and, false,
                    Collections.<MiruFieldFilter>emptyList(),
                    musts);
            } else {
                return new MiruFilter(MiruFilterOperation.and, false, Collections.<MiruFieldFilter>emptyList(), musts);
            }
        } else if (!shoulds.isEmpty()) {
            if (!mustNots.isEmpty()) {
                List<MiruFilter> filters = Lists.newArrayList();
                filters.add(wrap(musts, shoulds, Collections.<MiruFilter>emptyList()));
                filters.addAll(mustNots);
                return new MiruFilter(MiruFilterOperation.pButNotQ, false,
                    Collections.<MiruFieldFilter>emptyList(),
                    filters);
            } else {
                return new MiruFilter(MiruFilterOperation.or, false, Collections.<MiruFieldFilter>emptyList(), shoulds);
            }
        } else if (!mustNots.isEmpty()) {
            return new MiruFilter(MiruFilterOperation.pButNotQ, true, Collections.<MiruFieldFilter>emptyList(), mustNots);
        } else {
            throw new IllegalArgumentException("Nothing to filter");
        }
    }

}
