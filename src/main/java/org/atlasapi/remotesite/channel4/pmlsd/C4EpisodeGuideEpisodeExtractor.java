package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Episode;
import org.jdom.Element;

import com.metabroadcast.common.time.Clock;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;
import com.sun.syndication.feed.atom.Link;

final class C4EpisodeGuideEpisodeExtractor extends BaseC4EpisodeExtractor {

    public C4EpisodeGuideEpisodeExtractor(ContentFactory<Feed, Feed, Entry> contentFactory, Clock clock) {
        super(contentFactory, clock);
    }
    
    @Override
    protected Episode setAdditionalEpisodeFields(Entry entry, Map<String, String> lookup,
            Episode episode) {
        episode.addAliasUrl(C4AtomApi.canonicalizeEpisodeFeedId(entry));
        episode.addAliasUrl(entry.getId());
        String programmeId = lookup.get(C4AtomApi.DC_PROGRAMME_ID);
        if (programmeId != null) {
            Alias pid = new Alias("gb:channel4:programmeId", programmeId);
            episode.addAlias(pid);
        }
        return episode;
    }
    
    @Override
    @SuppressWarnings("unchecked")
    protected Element getMedia(Entry source) {
        for (Element element : (List<Element>) source.getForeignMarkup()) {
            if (C4AtomApi.NS_MEDIA_RSS.equals(element.getNamespace())
                && "content".equals(element.getName())) {
                return element;
            }
        }
        return null;
    }
}
