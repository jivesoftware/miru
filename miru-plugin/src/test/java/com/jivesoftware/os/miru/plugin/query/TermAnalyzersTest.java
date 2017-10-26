package com.jivesoftware.os.miru.plugin.query;

import com.google.common.collect.Lists;
import org.apache.lucene.analysis.Analyzer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TermAnalyzersTest {

    @Test
    public void testFindAnalyzer() throws Exception {
        TermAnalyzers termAnalyzers = new TermAnalyzers();
        for (String locale : Lists.newArrayList("en", "zh_cn", "foo_bar_bazz")) {
            Analyzer analyzer = termAnalyzers.findAnalyzer(locale, false);
            assertEquals("6.2.1", analyzer.getVersion().toString());
        }
    }

}
