package org.atlasapi.remotesite.wikipedia;

import org.junit.Test;

public class FootballTeamsExtractionTest {

    @Test
    public void testEpl() {
        EnglishWikipediaClient ewc = new EnglishWikipediaClient();
        for (String s : ewc.getAllTeamNames()) {
            System.out.println(s);
        }
    }
}
