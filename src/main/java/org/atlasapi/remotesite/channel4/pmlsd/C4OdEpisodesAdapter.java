package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.List;
import java.util.Optional;

import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Policy.Platform;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.SiteSpecificAdapter;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.metabroadcast.columbus.telescope.client.ModelWithPayload;
import com.metabroadcast.common.time.Clock;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;

public class C4OdEpisodesAdapter implements SiteSpecificAdapter<List<ModelWithPayload<Episode>>> {
    
    private static Logger log = LoggerFactory.getLogger(C4OdEpisodesAdapter.class);

    private final C4AtomApiClient feedClient;
    private final C4OnDemandEpisodeExtractor itemExtractor;

    public C4OdEpisodesAdapter(C4AtomApiClient feedClient, Optional<Platform> platform,
            ContentFactory<Feed, Feed, Entry> contentFactory, Publisher publisher, 
            C4LocationPolicyIds locationPolicyIds, boolean createIosBrandLocations, Clock clock) {
        this.feedClient = feedClient;
        this.itemExtractor = new C4OnDemandEpisodeExtractor(
                platform,
                publisher,
                contentFactory,
                locationPolicyIds,
                createIosBrandLocations,
                clock
        );
    }

    @Override
    public List<ModelWithPayload<Episode>> fetch(String uri) {
        Preconditions.checkArgument(canFetch(uri));
        Optional<Feed> brand4oDFeed = feedClient.brand4oDFeed(uri);
        
        List<ModelWithPayload<Episode>> episodes = Lists.newArrayList();
        
        if (brand4oDFeed.isPresent()) {
            for (Object entry : brand4oDFeed.get().getEntries()) {
                Entry oDEntry = (Entry)entry;
                try {
                    Episode episode = itemExtractor.extract(oDEntry);
                    if (episode != null) {
                        episodes.add(new ModelWithPayload<>(episode, oDEntry));
                    }
                } catch (Exception e) {
                    log.warn("Failed to extract Episode from entry " + oDEntry.getId() , e);
                }
            }
        }
        
        return episodes;
    }

    @Override
    public boolean canFetch(String uri) {
        return C4AtomApi.isACanonicalBrandUri(uri);
    }

}
