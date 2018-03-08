package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.Optional;
import java.util.Set;

import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.FetchException;
import org.atlasapi.remotesite.SiteSpecificAdapter;
import org.jdom.Element;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ImmutableSetMultimap.Builder;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.metabroadcast.common.time.Clock;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;

public class C4EpisodeGuideAdapter implements SiteSpecificAdapter<SetMultimap<Series,Episode>> {

    private static final String BRAND_FLATTENED_NAME = "relation.BrandFlattened";
    private static final String SERIES_NUMBER = "relation.SeriesNumber";
    
    private final C4AtomApiClient client;
    private final ContentExtractor<Feed, SetMultimap<Series,Episode>> seriesAndEpisodeExtractor;
    
    public C4EpisodeGuideAdapter(C4AtomApiClient client, ContentFactory<Feed, Feed, Entry> contentFactory, 
            Clock clock) {
        this.client = client;
        this.seriesAndEpisodeExtractor = new C4SeriesAndEpisodesExtractor(contentFactory, clock);
    }

    @Override
    public boolean canFetch(String uri) {
        return C4AtomApi.isACanonicalBrandUri(uri);
    }
    
    @Override
    public SetMultimap<Series,Episode> fetch(String uri) {
        Preconditions.checkArgument(canFetch(uri));
        try {
            Optional<Feed> episodeGuide = client.brandEpisodeGuideFeed(uri);
            
            if (episodeGuide.isPresent()) {
                return extractSeriesAndEpisodes(uri, episodeGuide.get());
            } else {
                return ImmutableSetMultimap.of();
            }
            
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private SetMultimap<Series,Episode> extractSeriesAndEpisodes(String uri, Feed feed) {
        if (isFlattenedBrandGuide(feed)) {
            return extractFromFlattenedEpg(uri, feed);
        } else {
            return extractFromFullEpg(uri, feed);
        }
    }

    private SetMultimap<Series,Episode> extractFromFullEpg(String uri, Feed feed) {
        Builder<Series, Episode> result = ImmutableSetMultimap.builder();
        for (Object entry : feed.getEntries()) {
            Optional<Feed> seriesFeed = fetchFeed(seriesNumber((Entry)entry), uri);
            if (seriesFeed.isPresent()) {
                result.putAll(seriesAndEpisodeExtractor.extract(seriesFeed.get()));
            }
        }
        return result.build();
    }

    private SetMultimap<Series,Episode> extractFromFlattenedEpg(String uri, Feed feed) {
        Set<Integer> seriesNumbers = seriesNumbers(feed);
        Builder<Series, Episode> result = ImmutableSetMultimap.builder();
        for (Integer seriesNumber : seriesNumbers) {
            Optional<Feed> seriesFeed = fetchFeed(seriesNumber, uri);
            if (seriesFeed.isPresent()) {
                result.putAll(seriesAndEpisodeExtractor.extract(seriesFeed.get()));
            }
        }
        return result.build();
    }

    private Set<Integer> seriesNumbers(Feed episodeGuide) {
        Set<Integer> seriesNumbers = Sets.newHashSet();
        for (Object entry : episodeGuide.getEntries()) {
            seriesNumbers.add(seriesNumber((Entry)entry));
        }
        return seriesNumbers;
    }

    private Optional<Feed> fetchFeed(int seriesNumber, String uri) {
        return client.seriesEpisodeGuideFeed(uri, seriesNumber);
    }
    
    @SuppressWarnings("unchecked")
    private int seriesNumber(Entry entry) {
        Iterable<Element> markup = (Iterable<Element>) entry.getForeignMarkup();
        for (Element element : markup) {
            if (SERIES_NUMBER.equals(element.getName())) {
                return Integer.parseInt(element.getValue());
            }
        }
        throw new FetchException("Couldn't find series number");
    }

    @SuppressWarnings("unchecked")
    private boolean isFlattenedBrandGuide(Feed feed) {
        Iterable<Element> markup = (Iterable<Element>) feed.getForeignMarkup();
        for (Element element : markup) {
            if (BRAND_FLATTENED_NAME.equals(element.getName()) && Boolean.valueOf(element.getValue())) {
                return true;
            }
        }
        return false;
    }
}
