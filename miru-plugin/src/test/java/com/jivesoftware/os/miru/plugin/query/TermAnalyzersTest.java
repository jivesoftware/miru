package com.jivesoftware.os.miru.plugin.query;

import com.google.common.collect.Lists;
import org.apache.lucene.analysis.Analyzer;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.*;

public class TermAnalyzersTest {
    @Test
    public void testFindAnalyzer() throws Exception {
        TermAnalyzers termAnalyzers = new TermAnalyzers();
        List<String> localeList = Lists.newArrayList("en", "zh_cn", "foo_bar_bazz");
        for (String locale : localeList) {
            Analyzer analyzer = termAnalyzers.findAnalyzer(locale, false);
            assertEquals("6.2.1", analyzer.getVersion().toString());
            System.out.println(locale + " v" + analyzer.getVersion());
        }
    }

}
