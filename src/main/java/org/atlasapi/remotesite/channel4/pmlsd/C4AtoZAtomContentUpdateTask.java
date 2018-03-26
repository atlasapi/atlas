package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.List;
import java.util.Optional;

import org.atlasapi.media.entity.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;
import com.sun.syndication.feed.atom.Link;

import javax.annotation.Nullable;

public class C4AtoZAtomContentUpdateTask extends ScheduledTask {
	
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final Iterable<Optional<Feed>> atozFeeds;
    private final C4BrandUpdater brandUpdater;
    private final Publisher publisher;
	
	private final C4LinkBrandNameExtractor linkExtractor = new C4LinkBrandNameExtractor();


	public C4AtoZAtomContentUpdateTask(SimpleHttpClient client, String apiBaseUrl, C4BrandUpdater brandUpdater,
	        Publisher publisher) {
	    this(client, apiBaseUrl, Optional.empty(), brandUpdater, publisher);
	}
	
    public C4AtoZAtomContentUpdateTask(
            SimpleHttpClient client,
            String apiBaseUrl,
            Optional<String> platform,
            C4BrandUpdater brandUpdater,
            Publisher publisher
    ) {
        this.brandUpdater = brandUpdater;
        this.publisher = publisher;
		this.atozFeeds = feedSource(client, apiBaseUrl, platform);
    }

    private Iterable<Optional<Feed>> feedSource(
            final SimpleHttpClient client,
            final String apiBaseUrl,
            final Optional<String> platform
    ) {
        return () -> new C4AtoZFeedIterator(client, apiBaseUrl, platform);
    }

    @Override
    public void runTask() {
        for (Optional<Feed> fetchedFeed : atozFeeds) {
            if (fetchedFeed.isPresent()) {
                Feed feed = fetchedFeed.get();
                log.info("Processing {}", feed.getId());
                loadAndSaveFromFeed(feed);
            }
        }
    }

	@SuppressWarnings("unchecked")
	private void loadAndSaveFromFeed(Feed feed) {
		for (Entry entry: (List<Entry>) feed.getEntries()) {
		    String brandUri = extractUriFromLinks(entry);
		    if (brandUri != null && brandUpdater.canFetch(brandUri)) {
		        writeBrand(brandUri);
		    }
		}
	}

    @Nullable private String extractUriFromLinks(Entry entry) {
        for (Object link : Iterables.concat(entry.getAlternateLinks(), entry.getOtherLinks())) {
            Optional<String> extracted = linkExtractor.c4CanonicalUriFrom(((Link)link).getHref());
            if (extracted.isPresent()) {
                return extracted.get();
            }
        }
        return null;
    }

    private void writeBrand(String brandUri) {
        try {
             brandUpdater.createOrUpdateBrand(brandUri);
        } catch (Exception e) {
            log.error("Failed to update brand " + brandUri, e);
        }
    }
    
}
