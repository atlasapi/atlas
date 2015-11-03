package org.atlasapi.remotesite.wikipedia.football;

import org.atlasapi.media.entity.*;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.wikipedia.Article;
import org.atlasapi.remotesite.wikipedia.SwebleHelper;
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
            addLink(team, info.website);
            if (info.name != null ) {
                team.setTitle(info.name);
            } else {
                team.setTitle(article.getTitle());
            }
            team.setImage(SwebleHelper.getWikiImage(info.image));
            return team;
        } catch (IOException | ParseException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void addLink(Organisation team, String website) {
        RelatedLink link = new RelatedLink.Builder(RelatedLink.LinkType.UNKNOWN, website).build();
        team.addRelatedLink(link);
    }
}
