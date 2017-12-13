package org.atlasapi.remotesite.bbc.nitro;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.atlas.glycerin.model.Broadcast;
import com.metabroadcast.atlas.glycerin.queries.AvailabilityEntityTypeOption;
import com.metabroadcast.atlas.glycerin.queries.EntityTypeOption;
import com.metabroadcast.atlas.glycerin.queries.MediaTypeOption;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.client.EntityType;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.status.api.EntityRef;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.bbc.BbcFeeds;
import org.atlasapi.reporting.OwlReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporterFactory;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;
import org.atlasapi.util.GroupLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.reporting.Utils.getMissingContentGenresStatus;
import static org.atlasapi.reporting.Utils.getMissingContentTitleStatus;
import static org.atlasapi.reporting.Utils.getMissingEpisodeNumberStatus;

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

        OwlTelescopeReporter telescope = OwlTelescopeReporterFactory.getInstance().getTelescopeReporter(
                OwlTelescopeReporters.BBC_NITRO_INGEST_OFFSCHEDULE,
                Event.Type.INGEST
        );
        OwlReporter owlReporter = new OwlReporter(telescope);
        owlReporter.getTelescopeReporter().startReporting();

        for (List<ModelWithPayload<Item>> itemsWithPayload : fetched) {
            reportStatus("Locking item IDs");
            Set<String> episodeIds = ImmutableSet.of();
            Set<String> containerIds = ImmutableSet.of();
            try {
                episodeIds = ImmutableSet.copyOf(itemsWithPayload.stream()
                        .map(ModelWithPayload::getModel)
                        .map(Item::getCanonicalUri)
                        .map(BbcFeeds::pidFrom)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));

                containerIds = topLevelContainerIds(itemsWithPayload);

                lock.lock(episodeIds);
                lock.lock(containerIds);

                reportStatus("Resolving items from Atlas");

                ImmutableSet<ModelWithPayload<Item>> resolvedItems = localOrRemoteFetcher.resolveItems(itemsWithPayload);


                ImmutableSet<ModelWithPayload<Series>> series =
                        localOrRemoteFetcher.resolveOrFetchSeries(resolvedItems).stream()
                                .filter(input -> input.getModel() instanceof Series)
                                .map(input -> input.asModelType(Series.class))
                                .collect(MoreCollectors.toImmutableSet());

                ImmutableSet<ModelWithPayload<Brand>> brands =
                        localOrRemoteFetcher.resolveOrFetchSeries(resolvedItems).stream()
                                .filter(input -> input.getModel() instanceof Brand)
                                .map(input -> input.asModelType(Brand.class))
                                .collect(MoreCollectors.toImmutableSet());

                reportStatus("Writing items");
                writeContent(resolvedItems, series, brands, owlReporter);
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
        owlReporter.getTelescopeReporter().endReporting();
    }

    private void writeContent(
            ImmutableSet<ModelWithPayload<Item>> itemsWithPayload,
            @Nullable Set<ModelWithPayload<Series>> series,
            @Nullable Set<ModelWithPayload<Brand>> brands,
            OwlReporter owlReporter
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
                        owlReporter.getTelescopeReporter().reportSuccessfulEvent(
                                brandWithPayload.getModel().getId(),
                                brandWithPayload.getModel().getAliases(),
                                EntityType.BRAND,
                                brandWithPayload.getPayload()
                        );

                        if (brandWithPayload.getModel().getTitle() == null ||
                                brandWithPayload.getModel().getTitle().isEmpty()){
                            owlReporter.getStatusReporter().updateStatus(
                                    EntityRef.Type.CONTENT,
                                    brandWithPayload.getModel().getId(),
                                    getMissingContentTitleStatus(
                                            EntityType.BRAND.toString(),
                                            brandWithPayload.getModel().getId(),
                                            owlReporter.getTelescopeReporter().getTaskId())
                            );
                        }

                        if (brandWithPayload.getModel().getGenres() == null || brandWithPayload.getModel().getGenres().isEmpty()) {
                            owlReporter.getStatusReporter().updateStatus(
                                    EntityRef.Type.CONTENT,
                                    brandWithPayload.getModel().getId(),
                                    getMissingContentGenresStatus(
                                            EntityType.BRAND.toString(),
                                            brandWithPayload.getModel().getId(),
                                            owlReporter.getTelescopeReporter().getTaskId())
                            );
                        }
                    } else {
                        owlReporter.getTelescopeReporter().reportFailedEvent(
                                "Atlas did not return an id after attempting to create or update this Brand",
                                EntityType.BRAND,
                                brandWithPayload.getPayload(), itemWithPayload.getPayload()
                        );
                    }
                }

                ModelWithPayload<Series> seriesWithPayload = getSeries(item, seriesIndex);
                if (seriesWithPayload != null) {
                    contentWriter.createOrUpdate(seriesWithPayload.getModel());
                    //report to telescope
                    if (seriesWithPayload.getModel().getId() != null) {
                        owlReporter.getTelescopeReporter().reportSuccessfulEvent(
                                seriesWithPayload.getModel().getId(),
                                seriesWithPayload.getModel().getAliases(),
                                EntityType.SERIES,
                                seriesWithPayload.getPayload()
                        );

                        if (seriesWithPayload.getModel().getTitle() == null ||
                                seriesWithPayload.getModel().getTitle().isEmpty()){
                            owlReporter.getStatusReporter().updateStatus(
                                    EntityRef.Type.CONTENT,
                                    seriesWithPayload.getModel().getId(),
                                    getMissingContentTitleStatus(
                                            EntityType.SERIES.toString(),
                                            seriesWithPayload.getModel().getId(),
                                            owlReporter.getTelescopeReporter().getTaskId())
                            );
                        }

                        if (seriesWithPayload.getModel().getGenres() == null || seriesWithPayload.getModel().getGenres().isEmpty()) {
                            owlReporter.getStatusReporter().updateStatus(
                                    EntityRef.Type.CONTENT,
                                    seriesWithPayload.getModel().getId(),
                                    getMissingContentGenresStatus(
                                            EntityType.SERIES.toString(),
                                            seriesWithPayload.getModel().getId(),
                                            owlReporter.getTelescopeReporter().getTaskId())
                            );
                        }
                    } else {
                        owlReporter.getTelescopeReporter().reportFailedEvent(
                                "Atlas did not return an id after attempting to create or update this Series",
                                EntityType.SERIES,
                                seriesWithPayload.getPayload(), itemWithPayload.getPayload()
                        );
                    }
                }
                contentWriter.createOrUpdate(item);
                //report to telescope
                if (item.getId() != null) {
                    owlReporter.getTelescopeReporter().reportSuccessfulEvent(
                            item.getId(),
                            item.getAliases(),
                            EntityType.ITEM,
                            itemWithPayload.getPayload()
                    );

                    if (item.getTitle() == null || item.getTitle().isEmpty()){
                        owlReporter.getStatusReporter().updateStatus(
                                EntityRef.Type.CONTENT,
                                item.getId(),
                                getMissingContentTitleStatus(
                                        EntityType.ITEM.toString(),
                                        item.getId(),
                                        owlReporter.getTelescopeReporter().getTaskId())
                        );
                    }

                    if (item.getGenres() == null || item.getGenres().isEmpty()) {
                        owlReporter.getStatusReporter().updateStatus(
                                EntityRef.Type.CONTENT,
                                item.getId(),
                                getMissingContentGenresStatus(
                                        EntityType.ITEM.toString(),
                                        item.getId(),
                                        owlReporter.getTelescopeReporter().getTaskId())
                        );
                    }

                    if (item instanceof Episode && ((Episode) item).getEpisodeNumber() == null) {
                        owlReporter.getStatusReporter().updateStatus(
                                EntityRef.Type.CONTENT,
                                item.getId(),
                                getMissingEpisodeNumberStatus(
                                        EntityType.EPISODE.toString(),
                                        item.getId(),
                                        owlReporter.getTelescopeReporter().getTaskId())
                        );
                    }
                } else {
                    owlReporter.getTelescopeReporter().reportFailedEvent(
                            "Atlas did not return an id after attempting to create or update this Item",
                            EntityType.ITEM,
                            itemWithPayload.getPayload()
                    );
                }
                written++;
            } catch (Exception e) {
                owlReporter.getTelescopeReporter().reportFailedEvent(
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
