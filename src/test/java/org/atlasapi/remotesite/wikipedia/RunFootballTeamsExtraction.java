package org.atlasapi.remotesite.wikipedia;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.persistence.content.organisation.OrganisationWriter;
import org.atlasapi.remotesite.wikipedia.football.FootballTeamsExtractor;
import org.atlasapi.remotesite.wikipedia.wikiparsers.FetchMeister;
import org.atlasapi.remotesite.wikipedia.testutils.LocallyCachingArticleFetcher;
import org.atlasapi.remotesite.wikipedia.updaters.FootballTeamsUpdater;

import java.net.URISyntaxException;

public class RunFootballTeamsExtraction {

    public static void main(String... args) throws URISyntaxException {
        EnglishWikipediaClient ewc = new EnglishWikipediaClient();
        new FootballTeamsUpdater(
                ewc,
                new FetchMeister(new LocallyCachingArticleFetcher(ewc, System.getProperty("user.home") + "/atlasTestCaches/wikipedia/teams")),
                new FootballTeamsExtractor(),
                new OrganisationWriter() {

                    @Override
                    public void updateOrganisationItems(Organisation organisation) {

                    }

                    @Override
                    public void createOrUpdateOrganisation(Organisation organisation) {
                        System.out.println(organisation.getTitle());
                        System.out.println(organisation.getPublisher());
                        System.out.println(organisation.getCanonicalUri());
                        System.out.println(organisation.getImage());
                        System.out.println(organisation.getRelatedLinks());
                        System.out.println(organisation.getAliases());
                    }
                },
                5,
                2
        ).run();
    }
}
