package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.List;
import java.util.Optional;

import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.SiteSpecificAdapter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metabroadcast.common.time.Clock;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;

public class C4BrandClipAdapter implements SiteSpecificAdapter<List<Clip>> {

    private C4AtomApiClient client;
    private C4BrandClipExtractor extractor;

    public C4BrandClipAdapter(C4AtomApiClient client, Publisher publisher, Clock clock, 
            ContentFactory<Feed, Feed, Entry> contentFactory, C4LocationPolicyIds locationPolicyIds) {
        this.client = client;
        this.extractor = new C4BrandClipExtractor(contentFactory, publisher, locationPolicyIds, new C4ContentLinker(), clock);
    }
    
    @Override
    public List<Clip> fetch(String uri) {
        Preconditions.checkArgument(C4AtomApi.isACanonicalBrandUri(uri));
        
        Optional<Feed> brandVideoFeed = client.brandVideoFeed(uri);
        
        List<Clip> clips = Lists.newArrayList();
        
        if (brandVideoFeed.isPresent()) {
            for (Object entry : brandVideoFeed.get().getEntries()) {
                Clip clip = extractor.extract((Entry)entry);
                if (clip != null) {
                    clips.add(clip);
                }
            }
        }
        
        return clips;
    }

    @Override
    public boolean canFetch(String uri) {
        return C4AtomApi.isACanonicalBrandUri(uri);
    }

}
