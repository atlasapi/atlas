package org.atlasapi.remotesite.bbc.nitro;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
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
import org.atlasapi.telescope.TelescopeProxy;
import org.atlasapi.telescope.TelescopeUtilityMethods;
import org.atlasapi.util.GroupLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * {@link NitroBroadcastHandler} which fetches, updates and writes relevant
 * content for the {@link Broadcast}.
 */
public class ContentUpdatingNitroBroadcastHandler implements NitroBroadcastHandler<ImmutableList<Optional<ItemRefAndBroadcast>>> {

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
    public ImmutableList<Optional<ItemRefAndBroadcast>> handle(Iterable<com.metabroadcast.atlas.glycerin.model.Broadcast> nitroBroadcasts, TelescopeProxy telescope) throws NitroException {

        Set<String> itemIds = itemIds(nitroBroadcasts);
        Set<String> containerIds = ImmutableSet.of();

        try {
            lock.lock(itemIds);
            ResolveOrFetchResult<Item> items = localOrRemoteFetcher.resolveOrFetchItem(nitroBroadcasts);

            containerIds = topLevelContainerIds(items.getAll());
            lock.lock(containerIds);

            ImmutableSet<Container> resolvedSeries = localOrRemoteFetcher.resolveOrFetchSeries(items.getAll());
            ImmutableSet<Container> resolvedBrands = localOrRemoteFetcher.resolveOrFetchBrand(items.getAll());

            Iterable<Series> series = Iterables.filter(Iterables.concat(resolvedSeries, resolvedBrands), Series.class);
            Iterable<Brand> brands = Iterables.filter(Iterables.concat(resolvedSeries, resolvedBrands), Brand.class);

            return writeContent(nitroBroadcasts, items, series, brands, telescope);
        } catch (InterruptedException ie) {
            return ImmutableList.of();
        } finally {
            lock.unlock(itemIds);
            lock.unlock(containerIds);
        }
    }

    private Set<String> itemIds(
            Iterable<com.metabroadcast.atlas.glycerin.model.Broadcast> nitroBroadcasts) {
        return ImmutableSet.copyOf(Iterables.transform(nitroBroadcasts,
                new Function<com.metabroadcast.atlas.glycerin.model.Broadcast, String>() {
                    @Override
                    public String apply(com.metabroadcast.atlas.glycerin.model.Broadcast input) {
                        return NitroUtil.programmePid(input).getPid();
                    }
                }
        ));
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


    private ImmutableList<Optional<ItemRefAndBroadcast>> writeContent(
            Iterable<com.metabroadcast.atlas.glycerin.model.Broadcast> nitroBroadcasts,
            ResolveOrFetchResult<Item> items, Iterable<Series> series,
            Iterable<Brand> brands,
            TelescopeProxy telescope) {
        ImmutableMap<String, Series> seriesIndex = Maps.uniqueIndex(series, Identified.TO_URI);
        ImmutableMap<String, Brand> brandIndex = Maps.uniqueIndex(brands, Identified.TO_URI);

        ImmutableList.Builder<Optional<ItemRefAndBroadcast>> results = ImmutableList.builder();

        for (com.metabroadcast.atlas.glycerin.model.Broadcast nitroBroadcast : nitroBroadcasts) {
            try {
                Optional<Broadcast> broadcast = broadcastExtractor.extract(nitroBroadcast);
                checkState(broadcast.isPresent(), "couldn't extract broadcast: %s", nitroBroadcast.getPid());

                String itemPid = NitroUtil.programmePid(nitroBroadcast).getPid();
                String itemUri = BbcFeeds.nitroUriForPid(itemPid);
                Item item = items.get(itemUri);
                checkNotNull(item, "No item for broadcast %s: %s", nitroBroadcast.getPid(), itemPid);

                addBroadcast(item, versionUri(nitroBroadcast), broadcast.get());

                //To report to telescope, we need atlasIds (which are created with the below codec.
                NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

                Brand brand = getBrand(item, brandIndex);
                if (brand != null) {
                    writer.createOrUpdate(brand);
                    //report to telescope
                    if (brand.getId() != null) {
                        String atlasId = idCodec.encode(BigInteger.valueOf(brand.getId()));
                        telescope.reportSuccessfulEvent(atlasId, TelescopeUtilityMethods.getAliases(brand.getAliases()), nitroBroadcast);
                    } else {
                        telescope.reportFailedEventWithWarning("BBC Nitro ingester failed to write brand in the DB", nitroBroadcast);
                    }
                }

                Series sery = getSeries(item, seriesIndex);
                if (sery != null) {
                    writer.createOrUpdate(sery);
                    //report to telescope
                    if (sery.getId() != null) {
                        String atlasId = idCodec.encode(BigInteger.valueOf(sery.getId()));
                        telescope.reportSuccessfulEvent(atlasId, TelescopeUtilityMethods.getAliases(sery.getAliases()), nitroBroadcast);
                    } else {
                        telescope.reportFailedEventWithWarning("BBC Nitro ingester failed to write series in the DB", nitroBroadcast);
                    }
                }

                writer.createOrUpdate(item);
                //report to telescope
                if (item.getId() != null) {
                    String atlasId = idCodec.encode(BigInteger.valueOf(item.getId()));
                    telescope.reportSuccessfulEvent(atlasId, TelescopeUtilityMethods.getAliases(item.getAliases()), nitroBroadcast);
                } else {
                    telescope.reportFailedEventWithWarning("BBC Nitro ingester failed to write item in the DB", nitroBroadcast);
                }

                results.add(Optional.of(new ItemRefAndBroadcast(item, broadcast.get())));
            } catch (Exception e) {
                log.error(nitroBroadcast.getPid(), e);
                telescope.reportFailedEventWithError("There was some kind of exception while the bbc Nitro ingester was writing to atlas. NitroBroadcast PID: " + nitroBroadcast.getPid(), nitroBroadcast);
                results.add(Optional.<ItemRefAndBroadcast>absent());
            }
        }
        return results.build();
    }

    private Series getSeries(Item item, ImmutableMap<String, Series> seriesIndex) {
        if (item instanceof Episode) {
            ParentRef container = ((Episode) item).getSeriesRef();
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

    private void addBroadcast(Item item, String versionUri, Broadcast broadcast) {
        Version version = Objects.firstNonNull(getVersion(item, versionUri), newVersion(versionUri));
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
