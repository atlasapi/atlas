package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.remotesite.ContentExtractor;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ImmutableSetMultimap.Builder;
import com.google.common.collect.SetMultimap;

import com.metabroadcast.columbus.telescope.client.ModelWithPayload;
import com.metabroadcast.common.time.Clock;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;

/**
 * Extracts content from a feed such as /episode-guide/series-x.atom for some x
 * where 0 < x <= |S|, S being the set of Series in a given Brand.
 * 
 * @author Fred van den Driessche (fred@metabroadcast.com)
 */
public class C4SeriesAndEpisodesExtractor implements
        ContentExtractor<Feed, SetMultimap<ModelWithPayload<Series>, ModelWithPayload<Episode>>> {

    private static final Pattern SERIES_ID = Pattern.compile("series-(\\d+)");

    private final C4EpisodeGuideEpisodeExtractor episodeExtractor;
    private final ContentFactory<Feed, Feed, Entry> contentFactory;
    private final Clock clock;

    public C4SeriesAndEpisodesExtractor(ContentFactory<Feed, Feed, Entry> contentFactory, Clock clock) {
        this.contentFactory = contentFactory;
        this.episodeExtractor = new C4EpisodeGuideEpisodeExtractor(contentFactory, clock);
        
        this.clock = clock;
    }

    @Override
    public SetMultimap<ModelWithPayload<Series>, ModelWithPayload<Episode>> extract(Feed source) {

        Series series = createSeriesFromFeed(source);
        
        Builder<ModelWithPayload<Series>, ModelWithPayload<Episode>> result =
                ImmutableSetMultimap.builder();
        for (Object entry : source.getEntries()) {
            result.put(
                    new ModelWithPayload<>(series, source),
                    extractEpisode(series, (Entry) entry)
            );
        }

        return result.build();
    }

    private Series createSeriesFromFeed(Feed source) {
        Series series = contentFactory.createSeries(source).get();
        
        series.addAliasUrl(C4AtomApi.canonicalizeSeriesFeedId(source));
        series.addAliasUrl(C4AtomApi.hierarchyUriFromCanonical(series.getCanonicalUri()));

        series.setLastUpdated(clock.now());
        
        series.withSeriesNumber(seriesNumberFrom(source));
        
        series.setTitle(source.getTitle());
        series.setDescription(source.getSubtitle().getValue());
        C4AtomApi.addImages(series, source.getLogo());
        
        series.setMediaType(MediaType.VIDEO);
        series.setSpecialization(Specialization.TV);
        
        return series;
    }
    
    private Integer seriesNumberFrom(Feed source) {
        Matcher matcher = SERIES_ID.matcher(source.getId());
        if (matcher.find()) {
            return Integer.valueOf(matcher.group(1));
        }
        return null;
    }
    
    private ModelWithPayload<Episode> extractEpisode(Series series, Entry entry) {
        Episode episode = episodeExtractor.extract(entry);
        episode.setSeries(series);
        return new ModelWithPayload<>(episode, entry);
    }

}
