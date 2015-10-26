package org.atlasapi.remotesite.wikipedia.football;

import com.google.common.collect.ImmutableList;
import org.atlasapi.media.entity.*;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.wikipedia.Article;
import org.atlasapi.remotesite.wikipedia.SwebleHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xtc.parser.ParseException;

import java.io.IOException;
import java.util.Map;

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
            ImmutableList<SwebleHelper.ListItemResult> name = info.clubname;
            if (name != null && name.size() == 1) {
                team.setTitle(name.get(0).name);
            }

            if (info.externalAliases != null) {
                for (Map.Entry<String, String> a : info.externalAliases.entrySet()) {
                    team.addAlias(new Alias(a.getKey(), a.getValue()));
                }
            }

            return team;
        } catch (IOException | ParseException ex) {
            throw new RuntimeException(ex);
        }
    }

}
