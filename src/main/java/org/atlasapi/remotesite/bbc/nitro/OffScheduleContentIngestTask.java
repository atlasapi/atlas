package org.atlasapi.remotesite.bbc.nitro;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.bbc.BbcFeeds;
import org.atlasapi.util.GroupLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metabroadcast.atlas.glycerin.queries.AvailabilityEntityTypeOption;
import com.metabroadcast.atlas.glycerin.queries.AvailabilityOption;
import com.metabroadcast.atlas.glycerin.queries.EntityTypeOption;
import com.metabroadcast.atlas.glycerin.queries.MediaTypeOption;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
import com.metabroadcast.common.scheduling.ScheduledTask;

public class OffScheduleContentIngestTask extends ScheduledTask {

    public static final Logger log = LoggerFactory.getLogger(OffScheduleContentIngestTask.class);

    private final NitroContentAdapter contentAdapter;
    private final int pageSize;
    private final ContentWriter contentWriter;
    private final LocalOrRemoteNitroFetcher localOrRemoteFetcher;
    private final GroupLock<String> lock;

    public OffScheduleContentIngestTask(
            NitroContentAdapter contentAdapter, int pageSize,
            ContentWriter contentWriter, GroupLock<String> lock,
            LocalOrRemoteNitroFetcher localOrRemoteFetcher
    ) {
        this.localOrRemoteFetcher = checkNotNull(localOrRemoteFetcher);
        this.lock = checkNotNull(lock);
        this.contentWriter = checkNotNull(contentWriter);
        this.contentAdapter = checkNotNull(contentAdapter);
        this.pageSize = pageSize;
    }

    @Override
    protected void runTask() {
        ProgrammesQuery query = ProgrammesQuery
                .builder()
                .withMixins(ProgrammesMixin.TITLES, ProgrammesMixin.PEOPLE)
                .withAvailability(AvailabilityOption.AVAILABLE)
                .withPageSize(pageSize)
                .withAvailabilityEntityType(AvailabilityEntityTypeOption.EPISODE)
                .withEntityType(EntityTypeOption.EPISODE)
                .withMediaSet("iptv-all")
                .withMediaType(MediaTypeOption.AUDIO_VIDEO)
                .build();

        Set<String> episodeIds = ImmutableSet.of();
        Set<String> containerIds = ImmutableSet.of();

        try {
            reportStatus("Doing the discovery call");
            ImmutableSet<Item> fetched = contentAdapter.fetchEpisodes(query);

            reportStatus(String.format("Got %d items from discovery", fetched.size()));

            episodeIds = ImmutableSet.copyOf(Iterables.transform(fetched,
                    new Function<Item, String>() {
                        @Override
                        public String apply(Item input) {
                            return BbcFeeds.pidFrom(input.getCanonicalUri());
                        }
                    }));
            containerIds = topLevelContainerIds(fetched);

            reportStatus("Locking item IDs");

            lock.lock(episodeIds);
            lock.lock(containerIds);

            reportStatus("Resolving items from Atlas");

            ResolveOrFetchResult<Item> items = localOrRemoteFetcher.resolveItems(fetched);

            ImmutableSet<Container> resolvedSeries = localOrRemoteFetcher.resolveOrFetchSeries(items.getAll());
            ImmutableSet<Container> resolvedBrands = localOrRemoteFetcher.resolveOrFetchBrand(items.getAll());

            Iterable<Series> series = Iterables.filter(Iterables.concat(resolvedSeries, resolvedBrands), Series.class);
            Iterable<Brand> brands = Iterables.filter(Iterables.concat(resolvedSeries, resolvedBrands), Brand.class);

            reportStatus("Writing items");
            writeContent(items, series, brands);
        } catch (NitroException e) {
            throw Throwables.propagate(e);
        } catch (InterruptedException e) {
            log.error("Could not lock item IDs", e);
        } finally {
            lock.unlock(episodeIds);
            lock.unlock(containerIds);
        }
    }


    private void writeContent(
            ResolveOrFetchResult<Item> items,
            Iterable<Series> series,
            Iterable<Brand> brands
    ) {
        ImmutableMap<String, Series> seriesIndex = Maps.uniqueIndex(series, Identified.TO_URI);
        ImmutableMap<String, Brand> brandIndex = Maps.uniqueIndex(brands, Identified.TO_URI);

        ImmutableSet<Item> allItems = items.getAll();
        int written = 0;
        int failed = 0;
        int total = allItems.size();
        for (Item item : allItems) {
            try {
                Brand brand = getBrand(item, brandIndex);
                if (brand != null) {
                    contentWriter.createOrUpdate(brand);
                }

                Series sery = getSeries(item, seriesIndex);
                if (sery != null) {
                    contentWriter.createOrUpdate(sery);
                }
                contentWriter.createOrUpdate(item);
                written++;
            } catch (Exception e) {
                log.error(item.getCanonicalUri(), e);
                failed++;
            }
            reportStatus(String.format("Written %d / %d items; %d failed", written, total, failed));
        }
    }

    private Series getSeries(Item item, ImmutableMap<String, Series> seriesIndex) {
        if (item instanceof Episode) {
            ParentRef container = ((Episode)item).getSeriesRef();
            if (container != null) {
                return seriesIndex.get(container.getUri());
            }
        }
        return null;
    }

    private Brand getBrand(Item item, ImmutableMap<String, Brand> brandIndex) {
        ParentRef container = item.getContainer();
        if (container != null) {
            return brandIndex.get(container.getUri());
        }
        return null;
    }

    private Set<String> topLevelContainerIds(ImmutableSet<Item> items) {
        return ImmutableSet.copyOf(Iterables.filter(Iterables.transform(items,
                new Function<Item, String>() {
                    @Override
                    public String apply(Item input) {
                        if (input.getContainer() != null) {
                            return input.getContainer().getUri();
                        }
                        return null;
                    }
                }
        ), Predicates.notNull()));
    }
}
