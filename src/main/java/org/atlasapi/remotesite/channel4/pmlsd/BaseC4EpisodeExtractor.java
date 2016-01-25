package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.Map;

import org.atlasapi.media.entity.Episode;

import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import com.metabroadcast.common.time.Clock;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;


public abstract class BaseC4EpisodeExtractor extends C4MediaItemExtractor<Episode> {

    private static final String DC_EPISODE_NUMBER = "dc:relation.EpisodeNumber";
    private static final String DC_SERIES_NUMBER = "dc:relation.SeriesNumber";
    
    private final ContentFactory<Feed, Feed, Entry> contentFactory;
    
    public BaseC4EpisodeExtractor(ContentFactory<Feed, Feed, Entry> contentFactory, Clock clock) {
        super(clock);
        this.contentFactory = contentFactory;
    }

    @Override
    protected final Episode createItem(Entry entry, Map<String, String> lookup) {
        return contentFactory.createEpisode(entry).get();
    }

    @Override
    protected final Episode setAdditionalItemFields(Entry entry, Map<String, String> lookup, Episode episode) {
        episode.setEpisodeNumber(Ints.tryParse(Strings.nullToEmpty(lookup.get(DC_EPISODE_NUMBER))));
        episode.setSeriesNumber(Ints.tryParse(Strings.nullToEmpty(lookup.get(DC_SERIES_NUMBER))));
        episode.setIsLongForm(true);
        return setAdditionalEpisodeFields(entry, lookup, episode);
    }

    protected abstract Episode setAdditionalEpisodeFields(Entry entry, Map<String, String> lookup, Episode episode);

}
