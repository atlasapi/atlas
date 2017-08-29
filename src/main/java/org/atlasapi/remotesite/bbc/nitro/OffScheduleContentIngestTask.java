package org.atlasapi.remotesite.bbc.nitro;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Episode;
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
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class OffScheduleContentIngestTask extends ScheduledTask {

    public static final Logger log = LoggerFactory.getLogger(OffScheduleContentIngestTask.class);

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
        Iterable<List<ModelWithPayload<Item>>> fetched;
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

        for (List<ModelWithPayload<Item>> itemsWithPayload : fetched) {
            reportStatus("Locking item IDs");
            Set<String> episodeIds = ImmutableSet.of();
            Set<String> containerIds = ImmutableSet.of();
            try {
                episodeIds = ImmutableSet.copyOf(itemsWithPayload.stream()
                        .map(input -> BbcFeeds.pidFrom(input.getModel().getCanonicalUri()))
                        .collect(Collectors.toList()));

                containerIds = topLevelContainerIds(itemsWithPayload);

                lock.lock(episodeIds);
                lock.lock(containerIds);

                reportStatus("Resolving items from Atlas");

                ImmutableSet<ModelWithPayload<Item>> resolvedItems = localOrRemoteFetcher.resolveItems(itemsWithPayload);


                Iterable<ModelWithPayload<Series>> series =
                        localOrRemoteFetcher.resolveOrFetchSeries(resolvedItems).stream()
                                .filter(input -> input.getModel() instanceof Series)
                                .map(input -> input.asModelType(Series.class))
                                .collect(MoreCollectors.toImmutableSet());

                Iterable<ModelWithPayload<Brand>> brands =
                        localOrRemoteFetcher.resolveOrFetchSeries(resolvedItems).stream()
                                .filter(input -> input.getModel() instanceof Brand)
                                .map(input -> input.asModelType(Brand.class))
                                .collect(MoreCollectors.toImmutableSet());

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
            ImmutableSet<ModelWithPayload<Item>> itemsWithPayload,
            @Nullable Iterable<ModelWithPayload<Series>> series,
            @Nullable Iterable<ModelWithPayload<Brand>> brands,
            OwlTelescopeReporter telescope
    ) {
        Map<String, ModelWithPayload<Series>> seriesIndex = LocalOrRemoteNitroFetcher.getIndex(series);
        Map<String, ModelWithPayload<Brand>> brandIndex = LocalOrRemoteNitroFetcher.getIndex(brands);

        for (ModelWithPayload<Item> itemWithPayload : itemsWithPayload) {
            Item item = itemWithPayload.getModel();
            try {
                ModelWithPayload<Brand> brandWithPayload = getBrand(item, brandIndex);
                if (brandWithPayload != null) {
                    contentWriter.createOrUpdate(brandWithPayload.getModel());
                    //report to telescope
                    if (brandWithPayload.getModel().getId() != null) {
                        telescope.reportSuccessfulEvent(
                                brandWithPayload.getModel().getId(),
                                brandWithPayload.getModel().getAliases(),
                                itemWithPayload.getPayload(), brandWithPayload.getPayload()
                        );
                    } else {
                        telescope.reportFailedEvent(
                                "Atlas did not return an id after attempting to create or update this Brand",
                                itemWithPayload.getPayload(), brandWithPayload.getPayload()
                        );
                    }
                }

                ModelWithPayload<Series> seriesWithPayload = getSeries(item, seriesIndex);
                if (seriesWithPayload != null) {
                    contentWriter.createOrUpdate(seriesWithPayload.getModel());
                    //report to telescope
                    if (seriesWithPayload.getModel().getId() != null) {
                        telescope.reportSuccessfulEvent(
                                seriesWithPayload.getModel().getId(),
                                seriesWithPayload.getModel().getAliases(),
                                itemWithPayload.getPayload(), seriesWithPayload.getPayload()
                        );
                    } else {
                        telescope.reportFailedEvent(
                                "Atlas did not return an id after attempting to create or update this Series",
                                itemWithPayload.getPayload()
                        );
                    }
                }
                contentWriter.createOrUpdate(item);
                //report to telescope
                if (item.getId() != null) {
                    telescope.reportSuccessfulEvent(
                            item.getId(),
                            item.getAliases(),
                            itemWithPayload.getPayload()
                    );
                } else {
                    telescope.reportFailedEvent(
                            "Atlas did not return an id after attempting to create or update this Item",
                            itemWithPayload.getPayload()
                    );
                }
                written++;
            } catch (Exception e) {
                telescope.reportFailedEvent(
                        "This item could not be written to Atlas. id=" + item.getId() + " (" + e.getMessage() + ")",
                        itemWithPayload.getPayload()
                );
                log.error(item.getCanonicalUri(), e);
                failed++;
            }
        }
    }

    private ModelWithPayload<Series> getSeries(Item item, Map<String,ModelWithPayload<Series>> seriesIndex) {
        if (item instanceof Episode) {
            ParentRef container = ((Episode)item).getSeriesRef();
            if (container != null) {
                return seriesIndex.get(container.getUri());
            }
        }
        return null;
    }

    private ModelWithPayload<Brand> getBrand(Item item, Map<String, ModelWithPayload<Brand>> brandIndex) {
        ParentRef container = item.getContainer();
        if (container != null) {
            return brandIndex.get(container.getUri());
        }
        return null;
    }

    private ImmutableSet<String> topLevelContainerIds(List<ModelWithPayload<Item>> items) {
        return items.stream()
                .map(ModelWithPayload::getModel)
                .map(Item::getContainer)
                .filter(Objects::nonNull)
                .map(ParentRef::getUri)
                .filter(Objects::nonNull)
                .collect(MoreCollectors.toImmutableSet());
    }
}
