package org.atlasapi.remotesite.bbc.nitro;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.bbc.BbcFeeds;
import org.atlasapi.util.GroupLock;

import com.metabroadcast.atlas.glycerin.queries.AvailabilityEntityTypeOption;
import com.metabroadcast.atlas.glycerin.queries.AvailabilityOption;
import com.metabroadcast.atlas.glycerin.queries.EntityTypeOption;
import com.metabroadcast.atlas.glycerin.queries.MediaTypeOption;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
import com.metabroadcast.common.scheduling.ScheduledTask;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
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
                .withMixins(ProgrammesMixin.ANCESTOR_TITLES, ProgrammesMixin.CONTRIBUTIONS,
                        ProgrammesMixin.IMAGES, ProgrammesMixin.GENRE_GROUPINGS)
                .withAvailability(AvailabilityOption.AVAILABLE)
                .withPageSize(pageSize)
                .withAvailabilityEntityType(AvailabilityEntityTypeOption.EPISODE)
                .withEntityType(EntityTypeOption.EPISODE)
                .withMediaSet("iptv-all")
                .withMediaType(MediaTypeOption.AUDIO_VIDEO)
                .build();

        reportStatus("Doing the discovery call");
        Iterable<Item> fetched;
        try {
            fetched = contentAdapter.fetchEpisodes(query);
        } catch (NitroException e) {
            throw Throwables.propagate(e);
        }

        int written = 0;
        int failed = 0;

        reportStatus("Writing items");
        for (Item fetchedItem : fetched) {
            Optional<String> possibleEpisodeId = getCanonicalUri(fetchedItem);
            Optional<String> possibleContainerId = topLevelContainerId(fetchedItem);

            if (!possibleEpisodeId.isPresent() || !possibleContainerId.isPresent()) {
                continue;
            }

            String episodeId = possibleEpisodeId.get();
            String containerId = possibleContainerId.get();
            reportStatus(String.format("Locking item ID - %s", episodeId));

            boolean writeSuccessful = false;
            try {
                lock.lock(episodeId);
                lock.lock(containerId);

                ResolveOrFetchResult<Item> item = localOrRemoteFetcher.resolveItems(
                        ImmutableList.of(fetchedItem));

                ImmutableSet<Container> resolvedSeries = localOrRemoteFetcher.resolveOrFetchSeries(
                        item.getAll());
                ImmutableSet<Container> resolvedBrands = localOrRemoteFetcher.resolveOrFetchBrand(
                        item.getAll());

                writeSuccessful = writeContent(
                        item,
                        (Series) Iterables.getOnlyElement(resolvedSeries, null),
                        (Brand) Iterables.getOnlyElement(resolvedBrands, null)
                );

            } catch (NitroException e) {
                throw Throwables.propagate(e);
            } catch (InterruptedException e) {
                log.error("Could not lock item IDs", e);
            } finally {
                if (writeSuccessful) {
                    written++;
                } else {
                    failed++;
                }

                lock.unlock(episodeId);
                lock.unlock(containerId);

                reportStatus(String.format(
                        "Written %d items of which %d failed",
                        written,
                        failed
                ));
            }
        }
    }


    private boolean writeContent(
            ResolveOrFetchResult<Item> items,
            @Nullable Series series,
            @Nullable Brand brand
    ) {
        Item item = Iterables.getOnlyElement(items.getAll());
        try {
            if (brand != null) {
                contentWriter.createOrUpdate(brand);
            }

            if (series != null) {
                contentWriter.createOrUpdate(series);
            }

            contentWriter.createOrUpdate(item);
            return true;
        } catch (Exception e) {
            log.error(item.getCanonicalUri(), e);
            return false;
        }
    }

    private Optional<String> topLevelContainerId(Item item) {
        Optional<String> uri = Optional.absent();
        if (item.getContainer() != null) {
            uri = Optional.of(item.getContainer().getUri());
        }
        return uri;
    }

    private Optional<String> getCanonicalUri(Item item) {
        Optional<String> canonicalUri = Optional.absent();
        if (item.getCanonicalUri() != null) {
            canonicalUri = Optional.of(BbcFeeds.pidFrom(item.getCanonicalUri()));
        }
        return canonicalUri;
    }
}
