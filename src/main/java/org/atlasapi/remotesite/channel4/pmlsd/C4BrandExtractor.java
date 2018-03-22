package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.List;
import java.util.Optional;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Policy.Platform;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.ContentExtractor;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.metabroadcast.common.time.SystemClock;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;
import com.sun.syndication.feed.atom.Link;

public class C4BrandExtractor implements ContentExtractor<Feed, BrandSeriesAndEpisodes>{

    private final C4BrandBasicDetailsExtractor basicDetailsExtractor;
    private final C4OdEpisodesAdapter fourOditemAdapter;
    private final C4EpisodeGuideAdapter episodeGuideAdapter;
    private final C4BrandEpgAdapter brandEpgAdatper;
    private final C4BrandClipAdapter clipAdapter;
    
    private final C4ContentLinker linker = new C4ContentLinker();
    private final C4LinkBrandNameExtractor brandNameExtractor = new C4LinkBrandNameExtractor();
    
    public C4BrandExtractor(C4AtomApiClient feedClient, Optional<Platform> platform, Publisher publisher,
            ChannelResolver channelResolver, ContentFactory<Feed, Feed, Entry> contentFactory,
            C4LocationPolicyIds locationPolicyIds, boolean createIosBrandLocations) {
        SystemClock clock = new SystemClock();
        C4AtomApi c4AtomApi = new C4AtomApi(channelResolver);
        this.basicDetailsExtractor = new C4BrandBasicDetailsExtractor(c4AtomApi, contentFactory, 
                clock);
        this.episodeGuideAdapter = new C4EpisodeGuideAdapter(feedClient, contentFactory, clock);
        this.fourOditemAdapter = new C4OdEpisodesAdapter(
                feedClient,
                platform,
                contentFactory,
                publisher,
                locationPolicyIds,
                createIosBrandLocations,
                clock
        );
        this.brandEpgAdatper = new C4BrandEpgAdapter(feedClient, clock, c4AtomApi, publisher);
        this.clipAdapter = new C4BrandClipAdapter(feedClient, publisher, clock, contentFactory, 
                locationPolicyIds);
    }

    /*
     * Extracts a full content hierarchy for a brand. Attempts to retrieve data
     * from /episode-guide.atom, /4od.atom, /epg.atom and /video.atom (in that order).
     */
    @Override
    public BrandSeriesAndEpisodes extract(Feed source) {
        
        Brand brand = basicDetailsExtractor.extract(source);
        
        SetMultimap<Series, Episode> seriesAndEpisodes = HashMultimap.create();
        
        String c4CanonicalUri = c4CanonicalBrandUri(source);
        seriesAndEpisodes.putAll(episodeGuideAdapter.fetch(c4CanonicalUri));

        List<Episode> fourOdContent = fourOditemAdapter.fetch(c4CanonicalUri);
        seriesAndEpisodes = linker.link4odToEpg(seriesAndEpisodes, fourOdContent);
        
        List<Episode> epgContent = brandEpgAdatper.fetch(c4CanonicalUri);
        seriesAndEpisodes = linker.populateBroadcasts(seriesAndEpisodes, epgContent);
        
        List<Clip> clips = clipAdapter.fetch(c4CanonicalUri);
        seriesAndEpisodes = linker.linkClipsToContent(seriesAndEpisodes, clips, brand);
        
        seriesAndEpisodes = setBrandProperties(seriesAndEpisodes, brand);

        return new BrandSeriesAndEpisodes(brand, seriesAndEpisodes);
    }


    private SetMultimap<Series, Episode> setBrandProperties(SetMultimap<Series, Episode> content, Brand brand) {
        for (Episode episode : content.values()) {
            if (equivalentTitles(brand, episode)) {
                setHierarchicalTitle(episode);
            }
            if (episode.getImage() == null) {
                episode.setImage(brand.getImage());
                episode.setThumbnail(brand.getThumbnail());
            }
            episode.setGenres(brand.getGenres());
        }
        return content;
    }

    private void setHierarchicalTitle(Episode episode) {
        if (episode.getSeriesNumber() != null && episode.getEpisodeNumber() != null) {
            episode.setTitle("Series " + episode.getSeriesNumber() + " Episode " + episode.getEpisodeNumber());
        }
    }

    private boolean equivalentTitles(Brand brand, Episode episode) {
		String notAlphanumeric = "[^\\d\\w]";
		return episode.getTitle().replaceAll(notAlphanumeric, "").equals(brand.getTitle().replaceAll(notAlphanumeric, ""));
	}
    
    private String c4CanonicalBrandUri(Feed feed) {
        for (Object link : Iterables.concat(feed.getAlternateLinks(), feed.getOtherLinks())) {
            Optional<String> uri = brandNameExtractor.c4CanonicalUriFrom(((Link)link).getHref());
            if (uri.isPresent()) {
                return uri.get();
            }
        }
        return null;
    }

}
