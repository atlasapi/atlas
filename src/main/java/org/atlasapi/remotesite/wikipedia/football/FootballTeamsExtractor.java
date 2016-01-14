package org.atlasapi.remotesite.wikipedia.football;

import com.google.common.collect.ImmutableList;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.wikipedia.wikiparsers.Article;
import org.atlasapi.remotesite.wikipedia.wikiparsers.SwebleHelper;
import xtc.parser.ParseException;

import java.io.IOException;

public class FootballTeamsExtractor implements ContentExtractor<Article, Organisation> {

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
            team.setAlternativeTitles(info.nicknames);
            addLink(team, info.website);
            if (info.name != null ) {
                team.setTitle(info.name);
            } else {
                team.setTitle(article.getTitle());
            }
            if (info.image != null) {
                Image image = new Image(SwebleHelper.getWikiImage(info.image));
                team.setImages(ImmutableList.of(image));
            }
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
