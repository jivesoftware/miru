package com.jivesoftware.os.wiki.miru.deployable.region;

/**
 * Created by jonathan.colt on 12/1/16.
 */
public class WikiQuerierProvider {

    private final MiruWikiQuerier miruWikiQuerier;
    private final ESWikiQuerier esWikiQuerier;

    public WikiQuerierProvider(MiruWikiQuerier miruWikiQuerier, ESWikiQuerier esWikiQuerier) {
        this.miruWikiQuerier = miruWikiQuerier;
        this.esWikiQuerier = esWikiQuerier;
    }


    public WikiQuerier get(String querier) {
        if (querier.equals("miru")) {
            return miruWikiQuerier;
        }
        if (querier.equals("es")) {
            return esWikiQuerier;
        }
        return null;
    }
}
