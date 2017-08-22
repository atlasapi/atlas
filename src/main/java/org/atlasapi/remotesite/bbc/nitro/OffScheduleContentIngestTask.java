package org.atlasapi.remotesite.bbc.nitro;

import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.bbc.BbcFeeds;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;
import org.atlasapi.util.GroupLock;

import com.metabroadcast.atlas.glycerin.model.Broadcast;
import com.metabroadcast.atlas.glycerin.queries.AvailabilityEntityTypeOption;
import com.metabroadcast.atlas.glycerin.queries.EntityTypeOption;
import com.metabroadcast.atlas.glycerin.queries.MediaTypeOption;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.common.scheduling.ScheduledTask;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class OffScheduleContentIngestTask extends ScheduledTask {

    public static final Logger log = LoggerFactory.getLogger(OffScheduleContentIngestTask.class);
    private static final Function<Container, Series> TO_SERIES = new Function<Container, Series>() {

        @Override
        public Series apply(Container input) {
            return (Series) input;
        }
    };
    private static final Function<Container, Brand> TO_BRANDS = new Function<Container, Brand>() {

        @Override
        public Brand apply(Container input) {
            return (Brand) input;
        }
    };

    private final NitroContentAdapter contentAdapter;
    private final int pageSize;
    private final ContentWriter contentWriter;
    private final LocalOrRemoteNitroFetcher localOrRemoteFetcher;
    private final GroupLock<String> lock;

    // We maintain the state while the task is running, after that following fields are reset to 0.
    // This has been done so that writeContent method can update these fields
    // which then are being used by runTask method to report the status.
    private int written = 0;
    private int failed = 0;

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

    private Function<Container, Series> toSeries() {
        return TO_SERIES;
    }

    private Function<Container, Brand> toBrands() {
        return TO_BRANDS;
    }

    @Override
    protected void runTask() {
        written = 0;
        failed = 0;

        ProgrammesQuery query = ProgrammesQuery
                .builder()
                .withMixins(
                        ProgrammesMixin.ANCESTOR_TITLES,
                        ProgrammesMixin.CONTRIBUTIONS,
                        ProgrammesMixin.IMAGES,
                        ProgrammesMixin.GENRE_GROUPINGS,
                        ProgrammesMixin.AVAILABLE_VERSIONS
                )
                .withUnsafeArbitrary("availability", "available", "PT12H")
                .withPageSize(pageSize)
                .withAvailabilityEntityType(AvailabilityEntityTypeOption.EPISODE)
                .withEntityType(EntityTypeOption.EPISODE)
                .withMediaSet("iptv-all")
                .withMediaType(MediaTypeOption.AUDIO_VIDEO)
                .build();

        reportStatus("Doing the discovery call");
        Iterable<List<Item>> fetched;
        try {
            fetched = contentAdapter.fetchEpisodes(
                    query,
                    ImmutableListMultimap.<String, Broadcast>of()
            );
        } catch (NitroException e) {
            throw Throwables.propagate(e);
        }

        reportStatus("Writing items");

        OwlTelescopeReporter telescope = OwlTelescopeReporter.create(
                OwlTelescopeReporters.BBC_NITRO_INGEST_OFFSCHEDULE,
                Event.Type.INGEST
        );
        telescope.startReporting();

        for (List<Item> items : fetched) {
            reportStatus("Locking item IDs");
            Set<String> episodeIds = ImmutableSet.of();
            Set<String> containerIds = ImmutableSet.of();
            try {
                episodeIds = ImmutableSet.copyOf(Iterables.transform(items,
                        new Function<Item, String>() {
                            @Override
                            public String apply(Item input) {
                                return BbcFeeds.pidFrom(input.getCanonicalUri());
                            }
                        }));

                containerIds = topLevelContainerIds(items);

                lock.lock(episodeIds);
                lock.lock(containerIds);

                reportStatus("Resolving items from Atlas");

                ResolveOrFetchResult<Item> resolvedItems = localOrRemoteFetcher.resolveItems(items);

                Iterable<Series> series = Iterables.transform(
                        localOrRemoteFetcher.resolveOrFetchSeries(resolvedItems.getAll()),
                        toSeries()
                );

                Iterable<Brand> brands = Iterables.transform(
                        localOrRemoteFetcher.resolveOrFetchBrand(resolvedItems.getAll()),
                        toBrands()
                );

                reportStatus("Writing items");
                writeContent(resolvedItems, series, brands, telescope);
            } catch (NitroException e) {
                log.error("Item fetching failed", e);
                throw Throwables.propagate(e);
            } catch (InterruptedException e) {
                log.error("Could not lock item IDs", e);
            } finally {
                lock.unlock(episodeIds);
                lock.unlock(containerIds);

                reportStatus(String.format(
                        "Written %d items of which %d failed",
                        written,
                        failed
                ));
            }
        }
        telescope.endReporting();
    }

    private void writeContent(
            ResolveOrFetchResult<Item> items,
            @Nullable Iterable<Series> series,
            @Nullable Iterable<Brand> brands,
            OwlTelescopeReporter telescope
    ) {
        ImmutableMap<String, Series> seriesIndex = Maps.uniqueIndex(series, Identified.TO_URI);
        ImmutableMap<String, Brand> brandIndex = Maps.uniqueIndex(brands, Identified.TO_URI);

        ImmutableSet<Item> allItems = items.getAll();
        for (Item item : allItems) {
            try {
                Brand brand = getBrand(item, brandIndex);
                if (brand != null) {
                    contentWriter.createOrUpdate(brand);
                    //report to telescope
                    if (brand.getId() != null) {
                        telescope.reportSuccessfulEvent(
                                brand.getId(),
                                brand.getAliases(),
                                item
                        );
                    } else {
                        telescope.reportFailedEvent(
                                "Atlas did not return an id after attempting to create or update this Brand",
                                item
                        );
                    }
                }

                Series sery = getSeries(item, seriesIndex);
                if (sery != null) {
                    contentWriter.createOrUpdate(sery);
                    //report to telescope
                    if (sery.getId() != null) {
                        telescope.reportSuccessfulEvent(
                                sery.getId(),
                                sery.getAliases(),
                                item
                        );
                    } else {
                        telescope.reportFailedEvent(
                                "Atlas did not return an id after attempting to create or update this Series",
                                item
                        );
                    }
                }
                contentWriter.createOrUpdate(item);
                //report to telescope
                if (item.getId() != null) {
                    telescope.reportSuccessfulEvent(
                            item.getId(),
                            item.getAliases(),
                            item
                    );
                } else {
                    telescope.reportFailedEvent(
                            "Atlas did not return an id after attempting to create or update this Item",
                            item
                    );
                }
                written++;
            } catch (Exception e) {
                telescope.reportFailedEvent(
                        "This item could not be written to Atlas. id=" + item.getId() + " (" + e.getMessage() + ")",
                        item
                );
                log.error(item.getCanonicalUri(), e);
                failed++;
            }
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

    private ImmutableSet<String> topLevelContainerIds(List<Item> items) {
        return ImmutableSet.copyOf(Iterables.filter(Iterables.transform(items,
                new Function<Item, String>() {
                    @Override
                    public String apply(Item item) {
                        if (item.getContainer() != null) {
                            return item.getContainer().getUri();
                        }
                        return null;
                    }
                }
        ), Predicates.notNull()));
    }
}
