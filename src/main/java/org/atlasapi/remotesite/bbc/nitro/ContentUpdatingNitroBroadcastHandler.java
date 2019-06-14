package org.atlasapi.remotesite.bbc.nitro;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.columbus.telescope.client.EntityType;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.status.api.EntityRef;
import com.metabroadcast.status.api.NewAlert;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.ScheduleEntry.ItemRefAndBroadcast;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.bbc.BbcFeeds;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroBroadcastExtractor;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroUtil;
import org.atlasapi.reporting.OwlReporter;
import org.atlasapi.util.GroupLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.metabroadcast.status.util.Utils.encode;
import static org.atlasapi.reporting.status.Utils.getPartialStatusForContent;

/**
 * {@link NitroBroadcastHandler} which fetches, updates and writes relevant
 * content for the {@link Broadcast}.
 */
public class ContentUpdatingNitroBroadcastHandler
        implements NitroBroadcastHandler<ImmutableList<Optional<ItemRefAndBroadcast>>> {

    private static final Logger log = LoggerFactory.getLogger(ContentUpdatingNitroBroadcastHandler.class);

    private final ContentWriter writer;
    private final LocalOrRemoteNitroFetcher localOrRemoteFetcher;
    private final GroupLock<String> lock;

    private final NitroBroadcastExtractor broadcastExtractor = new NitroBroadcastExtractor();

    public ContentUpdatingNitroBroadcastHandler(ContentResolver resolver, ContentWriter writer,
            LocalOrRemoteNitroFetcher localOrRemoteNitroFetcher, GroupLock<String> lock) {
        this.writer = writer;
        this.localOrRemoteFetcher = localOrRemoteNitroFetcher;
        this.lock = lock;
    }

    @Override
    public ImmutableList<Optional<ItemRefAndBroadcast>> handle(
            Iterable<com.metabroadcast.atlas.glycerin.model.Broadcast> nitroBroadcasts,
            OwlReporter owlReporter) throws NitroException {

        Set<String> itemIds = itemIds(nitroBroadcasts);
        Set<String> containerIds = ImmutableSet.of();

        try {
            lock.lock(itemIds);
            ImmutableSet<ModelWithPayload<Item>> items =
                    localOrRemoteFetcher.resolveOrFetchItem( nitroBroadcasts );

            containerIds = topLevelContainerIds(items);
            lock.lock(containerIds);

            ImmutableSet<ModelWithPayload<Container>> resolvedSeries = localOrRemoteFetcher.resolveOrFetchSeries(items);
            ImmutableSet<ModelWithPayload<Container>> resolvedBrands = localOrRemoteFetcher.resolveOrFetchBrand(items);

            Iterable<ModelWithPayload<Container>> seriesAndBrands =
                    Iterables.concat(resolvedSeries,resolvedBrands);


            ImmutableSet<ModelWithPayload<Series>> series =
                    Stream.concat(resolvedSeries.stream(), resolvedBrands.stream())
                            .filter(modelAndPayload -> modelAndPayload.getModel() instanceof Series)
                            //this throws class cast exceptions, but we just checked.
                            .map(modelAndPayload -> modelAndPayload.asModelType(Series.class))
                            .collect(MoreCollectors.toImmutableSet());


            ImmutableSet<ModelWithPayload<Brand>> brands =
                    Stream.concat(resolvedSeries.stream(), resolvedBrands.stream())
                            .filter(modelAndPayload -> modelAndPayload.getModel() instanceof Brand)
                            //this throws class cast exceptions, but we just checked.
                            .map(modelAndPayload -> modelAndPayload.asModelType(Brand.class))
                            .collect(MoreCollectors.toImmutableSet());

            return writeContent(nitroBroadcasts, items, series, brands, owlReporter);
        } catch (InterruptedException ie) {
            return ImmutableList.of();
        } catch (Exception e) {
            owlReporter.getTelescopeReporter().reportFailedEvent(
                    "An exception has prevented handling Nitro Broadcasts (" + e.toString() + ")",
                    nitroBroadcasts);
            log.error("An exception has prevented handling Nitro Broadcasts.", e);
            return ImmutableList.of();
        } finally {
            lock.unlock(itemIds);
            lock.unlock(containerIds);
        }
    }

    private Set<String> itemIds(
            Iterable<com.metabroadcast.atlas.glycerin.model.Broadcast> nitroBroadcasts) {
        return ImmutableSet.copyOf(Iterables.transform(
                nitroBroadcasts,
                input -> NitroUtil.programmePid(input).getPid()
        ));
    }

     private ImmutableSet<String> topLevelContainerIds(ImmutableSet<ModelWithPayload<Item>> items) {
        return items.stream().map(ModelWithPayload::getModel)
                .map(Item::getContainer)
                .filter(java.util.Objects::nonNull)
                .map(ParentRef::getUri)
                .filter(java.util.Objects::nonNull)
                .collect(MoreCollectors.toImmutableSet());
    }

    private ImmutableList<Optional<ItemRefAndBroadcast>> writeContent(
            Iterable<com.metabroadcast.atlas.glycerin.model.Broadcast> nitroBroadcasts,
            Set<ModelWithPayload<Item>> items,
            Set<ModelWithPayload<Series>> series,
            Set<ModelWithPayload<Brand>> brands,
            OwlReporter owlReporter) {

        Map<String, ModelWithPayload<Item>> itemIndex = LocalOrRemoteNitroFetcher.getIndex(items);
        Map<String, ModelWithPayload<Brand>> brandIndex = LocalOrRemoteNitroFetcher.getIndex(brands);
        Map<String, ModelWithPayload<Series>> seriesIndex = LocalOrRemoteNitroFetcher.getIndex(series);

        ImmutableList.Builder<Optional<ItemRefAndBroadcast>> results = ImmutableList.builder();

        for (com.metabroadcast.atlas.glycerin.model.Broadcast nitroBroadcast : nitroBroadcasts) {
            try {
                Optional<Broadcast> broadcast = broadcastExtractor.extract(nitroBroadcast);
                checkState(
                        broadcast.isPresent(),
                        "couldn't extract broadcast: %s",
                        nitroBroadcast.getPid()
                );

                String itemPid = NitroUtil.programmePid(nitroBroadcast).getPid();
                String itemUri = BbcFeeds.nitroUriForPid(itemPid);
                ModelWithPayload<Item> item = itemIndex.get(itemUri);
                checkNotNull(
                        item,
                        "No item for broadcast %s: %s",
                        nitroBroadcast.getPid(),
                        itemPid
                );

                addBroadcast(item.getModel(), versionUri(nitroBroadcast), broadcast.get());

                ModelWithPayload<Brand> brand = getBrand(item.getModel(), brandIndex);
                if (brand != null) {
                    writer.createOrUpdate(brand.getModel());
                    //report to telescope
                    if (brand.getModel().getId() != null) {
                        owlReporter.getTelescopeReporter().reportSuccessfulEvent(
                                brand.getModel().getId(),
                                brand.getModel().getAliases(),
                                EntityType.BRAND,
                                nitroBroadcast
                        );

                        if (Strings.isNullOrEmpty(brand.getModel().getTitle())) {
                            owlReporter.getStatusReporter().updateStatus(
                                    EntityRef.Type.CONTENT,
                                    brand.getModel(),
                                    getPartialStatusForContent(
                                            brand.getModel().getId(),
                                            owlReporter.getTelescopeReporter().getTaskId(),
                                            NewAlert.Key.Check.MISSING,
                                            NewAlert.Key.Field.TITLE,
                                            String.format("Content %s is missing a title.",
                                                    encode(brand.getModel().getId())
                                            ),
                                            EntityRef.Type.CONTENT,
                                            brand.getModel().getPublisher().key(),
                                            false
                                    )
                            );
                        } else {
                            owlReporter.getStatusReporter().updateStatus(
                                    EntityRef.Type.CONTENT,
                                    brand.getModel(),
                                    getPartialStatusForContent(
                                            brand.getModel().getId(),
                                            owlReporter.getTelescopeReporter().getTaskId(),
                                            NewAlert.Key.Check.MISSING,
                                            NewAlert.Key.Field.TITLE,
                                            null,
                                            EntityRef.Type.CONTENT,
                                            brand.getModel().getPublisher().key(),
                                            true
                                    )
                            );
                        }

                    } else {
                        owlReporter.getTelescopeReporter().reportFailedEvent(
                                "Atlas did not return an id after attempting to create or update this Brand",
                                EntityType.BRAND,
                                nitroBroadcast, item.getPayload(), brand.getPayload() //this might be an overkill
                        );
                    }
                }

                ModelWithPayload<Series> sery = getSeries(item.getModel(), seriesIndex);
                if (sery != null) {
                    writer.createOrUpdate(sery.getModel());
                    //report to telescope
                    if (sery.getModel().getId() != null) {
                        owlReporter.getTelescopeReporter().reportSuccessfulEvent(
                                sery.getModel().getId(),
                                sery.getModel().getAliases(),
                                EntityType.SERIES,
                                nitroBroadcast
                        );

                        if (Strings.isNullOrEmpty(sery.getModel().getTitle())){
                            owlReporter.getStatusReporter().updateStatus(
                                    EntityRef.Type.CONTENT,
                                    sery.getModel(),
                                    getPartialStatusForContent(
                                            sery.getModel().getId(),
                                            owlReporter.getTelescopeReporter().getTaskId(),
                                            NewAlert.Key.Check.MISSING,
                                            NewAlert.Key.Field.TITLE,
                                            String.format("Content %s is missing a title.",
                                                    encode(sery.getModel().getId())
                                            ),
                                            EntityRef.Type.CONTENT,
                                            sery.getModel().getPublisher().key(),
                                            false
                                    )
                            );
                        } else {
                            owlReporter.getStatusReporter().updateStatus(
                                    EntityRef.Type.CONTENT,
                                    sery.getModel(),
                                    getPartialStatusForContent(
                                            sery.getModel().getId(),
                                            owlReporter.getTelescopeReporter().getTaskId(),
                                            NewAlert.Key.Check.MISSING,
                                            NewAlert.Key.Field.TITLE,
                                            null,
                                            EntityRef.Type.CONTENT,
                                            sery.getModel().getPublisher().key(),
                                            true
                                    )
                            );
                        }

                    } else {
                        owlReporter.getTelescopeReporter().reportFailedEvent(
                                "Atlas did not return an id after attempting to create or update this Series",
                                EntityType.SERIES,
                                nitroBroadcast, item.getPayload(), sery.getPayload()
                        );
                    }
                }

                writer.createOrUpdate(item.getModel());
                //report to telescope
                if (item.getModel().getId() != null) {
                    owlReporter.getTelescopeReporter().reportSuccessfulEvent(
                            item.getModel().getId(),
                            item.getModel().getAliases(),
                            EntityType.ITEM,
                            nitroBroadcast
                    );

                    if (Strings.isNullOrEmpty(item.getModel().getTitle())){
                        owlReporter.getStatusReporter().updateStatus(
                                EntityRef.Type.CONTENT,
                                item.getModel(),
                                getPartialStatusForContent(
                                        item.getModel().getId(),
                                        owlReporter.getTelescopeReporter().getTaskId(),
                                        NewAlert.Key.Check.MISSING,
                                        NewAlert.Key.Field.TITLE,
                                        String.format("Content %s is missing a title.",
                                                encode(item.getModel().getId())
                                        ),
                                        EntityRef.Type.CONTENT,
                                        item.getModel().getPublisher().key(),
                                        false
                                )
                        );
                    } else {
                        owlReporter.getStatusReporter().updateStatus(
                                EntityRef.Type.CONTENT,
                                item.getModel(),
                                getPartialStatusForContent(
                                        item.getModel().getId(),
                                        owlReporter.getTelescopeReporter().getTaskId(),
                                        NewAlert.Key.Check.MISSING,
                                        NewAlert.Key.Field.TITLE,
                                        null,
                                        EntityRef.Type.CONTENT,
                                        item.getModel().getPublisher().key(),
                                        true
                                )
                        );
                    }

                    if (item.getModel() instanceof Episode && ((Episode) item.getModel()).getEpisodeNumber() == null) {
                        owlReporter.getStatusReporter().updateStatus(
                                EntityRef.Type.CONTENT,
                                item.getModel(),
                                getPartialStatusForContent(
                                        item.getModel().getId(),
                                        owlReporter.getTelescopeReporter().getTaskId(),
                                        NewAlert.Key.Check.MISSING,
                                        NewAlert.Key.Field.EPISODE_NUMBER,
                                        String.format("Content %s is missing an episode number.",
                                                encode(item.getModel().getId())
                                        ),
                                        EntityRef.Type.CONTENT,
                                        item.getModel().getPublisher().key(),
                                        false
                                )
                        );
                    } else { //TODO: Is this "else" correct?
                        owlReporter.getStatusReporter().updateStatus(
                                EntityRef.Type.CONTENT,
                                item.getModel(),
                                getPartialStatusForContent(
                                        item.getModel().getId(),
                                        owlReporter.getTelescopeReporter().getTaskId(),
                                        NewAlert.Key.Check.MISSING,
                                        NewAlert.Key.Field.EPISODE_NUMBER,
                                        null,
                                        EntityRef.Type.CONTENT,
                                        item.getModel().getPublisher().key(),
                                        true
                                )
                        );
                    }

                } else {
                    owlReporter.getTelescopeReporter().reportFailedEvent(
                            "Atlas did not return an id after attempting to create or update this Item",
                            EntityType.ITEM,
                            nitroBroadcast, item.getPayload()
                    );
                }

                results.add(Optional.of(new ItemRefAndBroadcast(item.getModel(), broadcast.get())));
            } catch (Exception e) {
                log.error(nitroBroadcast.getPid(), e);
                owlReporter.getTelescopeReporter().reportFailedEvent(
                        "An error has prevent content from being written to Atlas. (" + e.toString() + ")",
                        nitroBroadcast
                );
                results.add(Optional.<ItemRefAndBroadcast>absent());
            }
        }
        return results.build();
    }

    private ModelWithPayload<Series> getSeries(Item item, Map<String, ModelWithPayload<Series>> seriesIndex) {
        if (item instanceof Episode) {
            ParentRef container = ((Episode) item).getSeriesRef();
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

    private void addBroadcast(Item item, String versionUri, Broadcast broadcast) {
        Version version = Objects.firstNonNull(
                getVersion(item, versionUri),
                newVersion(versionUri)
        );
        version.setBroadcasts(Sets.union(ImmutableSet.of(broadcast), version.getBroadcasts()));
        item.addVersion(version);
    }

    private Version getVersion(Item item, String versionUri) {
        for (Version version : item.getVersions()) {
            if (versionUri.equals(version.getCanonicalUri())) {
                return version;
            }
        }
        return null;
    }

    private Version newVersion(String versionUri) {
        Version version = new Version();
        version.setCanonicalUri(versionUri);
        return version;
    }

    private String versionUri(com.metabroadcast.atlas.glycerin.model.Broadcast nitroBroadcast) {
        PidReference pidRef = NitroUtil.versionPid(nitroBroadcast);
        checkArgument(pidRef != null, "Broadcast %s has no version ref", nitroBroadcast.getPid());
        return BbcFeeds.nitroUriForPid(pidRef.getPid());
    }

}
