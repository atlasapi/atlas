package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.List;
import java.util.Optional;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.reporting.OwlReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporterFactory;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.client.ModelWithPayload;
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
    private final OwlReporter owlReporter;

	private final C4LinkBrandNameExtractor linkExtractor = new C4LinkBrandNameExtractor();


	public C4AtoZAtomContentUpdateTask(SimpleHttpClient client, String apiBaseUrl, C4BrandUpdater brandUpdater,
	        Publisher publisher, OwlReporter owlReporter) {
	    this(client, apiBaseUrl, Optional.empty(), brandUpdater, publisher, owlReporter);
	}

    public C4AtoZAtomContentUpdateTask(
            SimpleHttpClient client,
            String apiBaseUrl,
            Optional<String> platform,
            C4BrandUpdater brandUpdater,
            Publisher publisher,
            OwlReporter owlReporter
    ) {
        this.brandUpdater = brandUpdater;
        this.publisher = publisher;
		this.atozFeeds = feedSource(client, apiBaseUrl, platform);
		this.owlReporter = owlReporter;
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

	    owlReporter.getTelescopeReporter().startReporting();

        for (Optional<Feed> fetchedFeed : atozFeeds) {
            if (fetchedFeed.isPresent()) {
                Feed feed = fetchedFeed.get();
                log.info("Processing {}", feed.getId());
                loadAndSaveFromFeed(feed);
            }
        }

        owlReporter.getTelescopeReporter().endReporting();
    }

	@SuppressWarnings("unchecked")
    private void loadAndSaveFromFeed(Feed feed) {
        for (Entry entry : (List<Entry>) feed.getEntries()) {
            String brandUri = extractUriFromLinks(entry);
            if (brandUri != null && brandUpdater.canFetch(brandUri)) {
                ModelWithPayload<String> brandUriWithPayload = new ModelWithPayload<>(brandUri, entry);
                writeBrand(brandUriWithPayload);
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

    private void writeBrand(ModelWithPayload<String> brandUriWithPayload) {
        try {
             brandUpdater.createOrUpdateBrand(brandUriWithPayload);
        } catch (Exception e) {
            log.error("Failed to update brand " + brandUriWithPayload.getModel(), e);
        }
    }

}
