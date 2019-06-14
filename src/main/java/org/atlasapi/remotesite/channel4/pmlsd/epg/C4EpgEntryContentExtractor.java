package org.atlasapi.remotesite.channel4.pmlsd.epg;

import java.util.Optional;
import java.util.Set;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.channel4.pmlsd.C4AtomApi;
import org.atlasapi.remotesite.channel4.pmlsd.C4BrandUpdater;
import org.atlasapi.remotesite.channel4.pmlsd.C4LocationPolicyIds;
import org.atlasapi.remotesite.channel4.pmlsd.ContentFactory;
import org.atlasapi.remotesite.channel4.pmlsd.epg.model.C4EpgEntry;
import org.joda.time.DateTime;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;

public class C4EpgEntryContentExtractor implements
        ContentExtractor<C4EpgChannelEntry, ContentHierarchyAndBroadcast> {

    private final C4EpgEntryContentResolver resolver;
    private final C4BrandUpdater brandUpdater;
    private final Clock clock;

    private final C4EpgEntryBroadcastExtractor broadcastExtractor = new C4EpgEntryBroadcastExtractor();
    private final C4EpgEntryBrandExtractor brandExtractor;
    private final C4EpgEntrySeriesExtractor seriesExtractor;
    private final C4EpgEntryItemExtractor itemExtractor;
    
    public C4EpgEntryContentExtractor(ContentResolver resolver, C4BrandUpdater updater, 
            ContentFactory<C4EpgEntry, C4EpgEntry, C4EpgEntry> contentFactory, 
            Publisher publisher, C4LocationPolicyIds locationPolicyIds, Clock clock) {
        this.resolver =  new C4EpgEntryContentResolver(resolver, publisher);
        this.brandUpdater = new C4EpgRelatedLinkBrandUpdater(updater);
        this.brandExtractor = new C4EpgEntryBrandExtractor(contentFactory);
        this.itemExtractor = new C4EpgEntryItemExtractor(contentFactory, locationPolicyIds, clock);
        this.seriesExtractor = new C4EpgEntrySeriesExtractor(contentFactory);
        this.clock = clock;
    }

    public C4EpgEntryContentExtractor(ContentResolver contentStore, C4BrandUpdater brandUpdater, 
            ContentFactory<C4EpgEntry, C4EpgEntry, C4EpgEntry> contentFactory, 
            Publisher publisher, C4LocationPolicyIds locationPolicyIds) {
        this(contentStore, brandUpdater, contentFactory, publisher, locationPolicyIds, new SystemClock());
    }

    @Override
    public ContentHierarchyAndBroadcast extract(C4EpgChannelEntry source) {
        DateTime now = clock.now();

        Optional<Brand> brand = resolveBrand(source)
                .map(Optional::of).orElseGet(() -> fetchBrand(source))
                .map(Optional::of).orElseGet(() -> createBrandFrom(source));
        ensureHierarchyAlias(brand);
        
        Optional<Series> series = resolveSeries(source)
                .map(Optional::of).orElseGet(() -> createSeriesFrom(source));
        ensureHierarchyAlias(series);
        
        Item item = resolveItem(source).orElseGet(() -> createItem(source, brand, series));
        ensureHierarchyUri(item);
        
        Broadcast broadcast = broadcastExtractor.extract(source);
        broadcast.setLastUpdated(now);
        
        addOrReplaceBroadcast(item, broadcast);
        
        connectHeirarchy(item, series, brand);

        return new ContentHierarchyAndBroadcast(brand, series, item, broadcast);
    }

    private void ensureHierarchyAlias(Optional<? extends Content> possibleContent) {
        if (possibleContent.isPresent()) {
            ensureHierarchyUri(possibleContent.get());
        }
    }

    private void ensureHierarchyUri(Content content) {
        String uri = content.getCanonicalUri();
        content.addAliasUrl(C4AtomApi.hierarchyUriFromCanonical(uri));
    }

    private void connectHeirarchy(Item item, Optional<Series> series, Optional<Brand> brand) {
        if (brand.isPresent()) {
            item.setContainer(brand.get());
            if (series.isPresent()) {
                series.get().setParent(brand.get());
                if (item instanceof Episode) {
                    ((Episode) item).setSeries(series.get());
                }
            }
        }
    }

    private void addOrReplaceBroadcast(Item item, Broadcast broadcast) {
        Version version = Iterables.getOnlyElement(item.nativeVersions(), new Version());
        
        Broadcast existingBroadcast = existingBroadcast(version, broadcast);
        if (existingBroadcast != null && changed(broadcast, existingBroadcast)) {
            Set<Broadcast> broadcasts = Sets.newHashSet(broadcast);
            broadcasts.addAll(version.getBroadcasts());
            version.setBroadcasts(broadcasts);
        } else {
            version.addBroadcast(broadcast);
        }
    }
    
    private static boolean changed(Broadcast newBroadcast, Broadcast broadcast) {
        return !Objects.equal(newBroadcast.getTransmissionTime(),broadcast.getTransmissionTime())
            || !Objects.equal(newBroadcast.getTransmissionEndTime(), broadcast.getTransmissionEndTime())
            || !Objects.equal(newBroadcast.getBroadcastDuration(), broadcast.getBroadcastDuration());
    }

    private Broadcast existingBroadcast(Version version, Broadcast newBroadcast) {
        for (Broadcast broadcast : version.getBroadcasts()) {
            if (broadcast.getSourceId().equals(newBroadcast.getSourceId())) {
                return broadcast;
            }
        }
        return null;
    }

    private Item createItem(C4EpgChannelEntry source, Optional<Brand> brand, Optional<Series> series) {
        return itemExtractor.extract(new C4EpgEntryItemSource(source, brand, series));
    }

    private Optional<Item> resolveItem(C4EpgChannelEntry source) {
        return resolver.itemFor(source.getEpgEntry());
    }

    private Optional<Series> createSeriesFrom(C4EpgChannelEntry source) {
        return seriesExtractor.extract(source.getEpgEntry());
    }

    private Optional<Series> resolveSeries(C4EpgChannelEntry source) {
        return resolver.resolveSeries(source.getEpgEntry());
    }

    private Optional<Brand> createBrandFrom(C4EpgChannelEntry source) {
        return brandExtractor.extract(source.getEpgEntry());
    }

    private Optional<Brand> fetchBrand(C4EpgChannelEntry source) {
        return source.hasRelatedLink()  ? fetchBrand(source.getRelatedLinkUri()) 
                                        : Optional.empty();
    }

    private Optional<Brand> fetchBrand(String relatedLinkUri) {
        return Optional.ofNullable(brandUpdater.createOrUpdateBrand(relatedLinkUri));
    }

    private Optional<Brand> resolveBrand(C4EpgChannelEntry source) {
        return resolver.resolveBrand(source.getEpgEntry());
    }


}
