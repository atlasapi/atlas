package org.atlasapi.remotesite.wikipedia;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.wikipedia.film.FilmExtractor;
import org.atlasapi.remotesite.wikipedia.wikiparsers.FetchMeister;
import org.atlasapi.remotesite.wikipedia.testutils.LocallyCachingArticleFetcher;
import org.atlasapi.remotesite.wikipedia.updaters.FilmsUpdater;

public class RunFilmExtraction {
    public static void main(String... args) {
        EnglishWikipediaClient ewc = new EnglishWikipediaClient();
        new FilmsUpdater(
                ewc,
                new FetchMeister(new LocallyCachingArticleFetcher(ewc, System.getProperty("user.home") + "/atlasTestCaches/wikipedia/films")),
                new FilmExtractor(),
                new ContentWriter() {
                    @Override
                    public void createOrUpdate(Container container) {

                    }
                    @Override
                    public Item createOrUpdate(Item item) {
                        return item;
                    }
                },
                5,
                2
        ).run();
    }
}
