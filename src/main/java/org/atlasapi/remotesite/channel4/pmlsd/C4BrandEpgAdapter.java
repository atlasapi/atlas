package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.List;
import java.util.Optional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.SiteSpecificAdapter;

import com.google.common.collect.Lists;
import com.metabroadcast.common.time.Clock;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;

public class C4BrandEpgAdapter implements SiteSpecificAdapter<List<Episode>> {

    private final Log log = LogFactory.getLog(getClass());
    
    private final C4AtomApiClient client;
    private final C4EpgEpisodeExtractor extractor;

    public C4BrandEpgAdapter(C4AtomApiClient client, Clock clock, C4AtomApi atomApi, 
            Publisher publisher) {
        this.client = client;
        this.extractor = new C4EpgEpisodeExtractor(atomApi, 
                new SourceSpecificContentFactory<>(publisher, new C4BrandEpgUriExtractor()), 
                clock);
    }
    
    @Override
    public List<Episode> fetch(String uri) {
        
        Optional<Feed> epgFeed = client.brandEpgFeed(uri);

        List<Episode> episodes = Lists.newArrayList();
        
        if (epgFeed.isPresent()) {
            for (Object entry : epgFeed.get().getEntries()) {
                Entry atomEntry = (Entry)entry;
                try {
                    episodes.add(extractor.extract(atomEntry));
                } catch (Exception e) {
                    log.warn(String.format("Failed to extract episode from %s for brand %s", atomEntry.getId(), uri), e);
                }
            }
        }
        
        return episodes;
    }

    @Override
    public boolean canFetch(String uri) {
        return C4AtomApi.isACanonicalBrandUri(uri);
    };
}
