package org.atlasapi.remotesite.wikipedia.football;

import com.google.common.base.Strings;
import static com.google.common.base.Preconditions.checkNotNull;
import org.atlasapi.media.entity.*;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.wikipedia.Article;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xtc.parser.ParseException;

import java.io.IOException;

public class FootballTeamsExtractor implements ContentExtractor<Article, Organisation> {
    private static final Logger log = LoggerFactory.getLogger(FootballTeamsExtractor.class);

    @Override
    public Organisation extract(Article article) {
        String source = article.getMediaWikiSource();
        try {
            TeamInfoboxScrapper.Result info = TeamInfoboxScrapper.getInfoboxAttrs(source);
            String url = article.getUrl();
            Organisation team = new Organisation();
            team.setPublisher(Publisher.WIKIPEDIA);
            team.setLastUpdated(article.getLastModified());
            team.setCanonicalUri(url);
            team.setTitle(info.name);
            team.setImage(info.image);

            return team;
        } catch (IOException | ParseException ex) {
            throw new RuntimeException(ex);
        }
    }

}
