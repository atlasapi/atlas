package org.atlasapi.remotesite.bbc.nitro;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.bbc.BbcFeeds;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporterFactory;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;

import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.client.EntityType;
import com.metabroadcast.common.http.HttpStatusCode;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.ANCESTOR_TITLES;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.AVAILABLE_VERSIONS;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.CONTRIBUTIONS;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.GENRE_GROUPINGS;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.IMAGES;

@Controller
public class NitroForceUpdateController {

    private static final Logger log = LoggerFactory.getLogger(NitroForceUpdateController.class);

    private final NitroContentAdapter contentAdapter;
    private final NitroChannelAdapter channelAdapter;
    private final ContentWriter contentWriter;
    private final ChannelWriter channelWriter;
    private final LocalOrRemoteNitroFetcher localOrRemoteNitroFetcher;

    public NitroForceUpdateController(
            NitroContentAdapter contentAdapter,
            NitroChannelAdapter channelAdapter,
            ContentWriter contentWriter,
            ChannelWriter channelWriter,
            LocalOrRemoteNitroFetcher localOrRemoteNitroFetcher
    ) {
        this.contentAdapter = checkNotNull(contentAdapter);
        this.channelAdapter = checkNotNull(channelAdapter);
        this.contentWriter = checkNotNull(contentWriter);
        this.channelWriter = checkNotNull(channelWriter);
        this.localOrRemoteNitroFetcher = checkNotNull(localOrRemoteNitroFetcher);
    }

    @RequestMapping(value = "/system/bbc/nitro/merge/content/{pid}", method = RequestMethod.POST)
    public void mergeUpdatePidFromNitro(
            HttpServletResponse response,
            @PathVariable("pid") String pid
    ) throws IOException {
        OwlTelescopeReporter telescope = OwlTelescopeReporterFactory.getInstance().getTelescopeReporter(
                OwlTelescopeReporters.BBC_NITRO_INGEST_API,
                Event.Type.INGEST
        );
        telescope.startReporting();

        Iterable<List<ModelWithPayload<Item>>> itemListIterable = null;
        ModelWithPayload<Item> itemWithPayload;
        try { //to catch generic exceptions, and properly close telescope.
            ImmutableSet<ModelWithPayload<Item>> resolvedItems;
            try {
                itemListIterable = contentAdapter
                        .fetchEpisodes(
                                ProgrammesQuery.builder()
                                        .withPid(pid)
                                        .withMixins(
                                                ANCESTOR_TITLES,
                                                CONTRIBUTIONS,
                                                IMAGES,
                                                GENRE_GROUPINGS,
                                                AVAILABLE_VERSIONS
                                        )
                                        .withPageSize(1)
                                        .build(),
                                null
                        );

                Iterable<ModelWithPayload<Item>> allItems = Iterables.concat(itemListIterable);
                ModelWithPayload<Item> tmpItemWithPayload = Iterables.getOnlyElement(allItems);
                resolvedItems = localOrRemoteNitroFetcher.resolveItems(ImmutableList.of(
                        tmpItemWithPayload));
                itemWithPayload = Iterables.getOnlyElement(resolvedItems);

            } catch (NitroException e) {
                log.error("Failed to get Nitro item {}", pid, e);
                writeServerErrorWithStack(response, e);
                telescope.reportFailedEvent(
                        "The request at 'bbc/nitro/merge/content/"+pid+"' failed. "+
                        " (" + e.getMessage() + ")");
                telescope.endReporting();
                return;
            } catch (NoSuchElementException e) {
                log.error("No items found in Nitro for pid {}", pid);
                response.setStatus(HttpStatusCode.NOT_FOUND.code());
                response.setContentLength(0);
                telescope.reportFailedEvent(
                        "The request at 'bbc/nitro/merge/content/"+pid+"' failed. "+
                        "No items found in Nitro for pid=" + pid);
                telescope.endReporting();
                return;
            } catch (IllegalArgumentException e) {
                //this might also be caused if resolved items dont merge properly
                String message = String.format(
                        "The request at 'bbc/nitro/merge/content/"+pid+"' failed. "+
                        "Got more than 1 item from Nitro for pid %s", pid);
                log.error(message, e);
                telescope.reportFailedEvent(message, itemListIterable);
                response.setStatus(HttpStatusCode.SERVER_ERROR.code());
                response.setContentLength(message.length());
                response.getWriter().write(message);
                telescope.endReporting();
                return;
            }

        } catch (Exception e) {
            log.error("Exception while getting item for pid={}", pid, e);
            telescope.reportFailedEvent(
                    "The request at 'bbc/nitro/merge/content/"+pid+"' failed. "+
                    " (" + e.toString() + ")");
            writeServerErrorWithStack(response, e);
            telescope.endReporting();
            return;
        }

        createOrUpdateItem(response, pid, itemWithPayload, telescope);
        telescope.endReporting();
    }

    @RequestMapping(value = "/system/bbc/nitro/update/{type}/{pid}", method = RequestMethod.POST)
    public void updatePidFromNitro(
            HttpServletResponse response,
            @PathVariable("type") String type,
            @PathVariable("pid") String pid
    ) throws IOException {
        OwlTelescopeReporter telescope = OwlTelescopeReporterFactory.getInstance().getTelescopeReporter(
                OwlTelescopeReporters.BBC_NITRO_INGEST_API,
                Event.Type.INGEST
        );
        telescope.startReporting();

        try {
            switch (type) {
            case "content":
                forceUpdateContent(response, pid, telescope);
                break;
            case "service":
                forceUpdateChannel(response, this::fetchService, pid, telescope);
                break;
            case "masterbrand":
                forceUpdateChannel(response, this::fetchMasterbrand, pid, telescope);
                break;
            default:
                throw new IllegalArgumentException(String.format("Bad type %s", type));
            }
        } catch (IOException e) {
            log.error("Failed to get Nitro thing {}", pid, e);
            telescope.reportFailedEvent(
                    "The request at 'bbc/nitro/update/"+type+"/"+pid+"' failed. "+
                    "Failed to get Nitro item for pid=" + pid + " (" + e.getMessage() + ")");
            writeServerErrorWithStack(response, e);
        } catch (Exception e) {
            log.error("Exception while getting item for pid={}", pid, e);
            telescope.reportFailedEvent(
                    "The request at 'bbc/nitro/update/" + type + "/" + pid + "' failed. " +
                    " (" + e.toString() + ")");
            writeServerErrorWithStack(response, e);
        } finally {
            telescope.endReporting();
        }
    }

    private void forceUpdateChannel(
            HttpServletResponse response,
            Function<String, Optional<ModelWithPayload<Channel>>> channelResolve,
            String pid,
            OwlTelescopeReporter telescope
    ) throws IOException {
        Optional<ModelWithPayload<Channel>> channelWithPayload = channelResolve.apply(pid);
        if (!channelWithPayload.isPresent()) {
            log.error("No items found in Nitro for id {}", pid);
            telescope.reportFailedEvent("No items found in Nitro for pid=" + pid);
            response.setStatus(HttpStatusCode.NOT_FOUND.code());
            response.setContentLength(0);
        } else {
            Channel channel = channelWithPayload.get().getModel();
            channelWriter.createOrUpdate(channel);
            response.setStatus(HttpStatusCode.ACCEPTED.code());
            if(channel.getId() != null) {
                telescope.reportSuccessfulEvent(
                        channel.getId(),
                        channel.getAliases(),
                        EntityType.CHANNEL,
                        channelWithPayload.get().getPayload());
            } else {
                telescope.reportFailedEvent(
                        "There was an error while trying to write this Channel to Atlas. Channel pid=" + pid,
                        EntityType.CHANNEL,
                        channelWithPayload.get().getPayload()
                );
            }
        }
    }

    private void forceUpdateContent(
            HttpServletResponse response, String pid,
            OwlTelescopeReporter telescope)
            throws IOException {
        Iterable<List<ModelWithPayload<Item>>> itemsWithPayload;
        ModelWithPayload<Item> itemWithPayload;
        try {
            itemsWithPayload = contentAdapter
                    .fetchEpisodes(
                            ProgrammesQuery.builder()
                                    .withPid(pid)
                                    .withMixins(
                                            ANCESTOR_TITLES,
                                            CONTRIBUTIONS,
                                            IMAGES,
                                            GENRE_GROUPINGS,
                                            AVAILABLE_VERSIONS
                                    )
                                    .withPageSize(1)
                                    .build(),
                            null
                    );
        }catch (NitroException e) {
            log.error("Failed to get Nitro item {}", pid, e);
            telescope.reportFailedEvent(
                    "Failed to get Nitro item for pid=" + pid + " (" + e.getMessage() + ")");
            writeServerErrorWithStack(response, e);
            return;
        }

        try {
            itemWithPayload = Iterables.getOnlyElement(itemsWithPayload).get(0);
        } catch (NoSuchElementException | IndexOutOfBoundsException e ) {
            log.error("No items found in Nitro for pid {}", pid);
            telescope.reportFailedEvent("No items found in Nitro for pid=" + pid);
            response.setStatus(HttpStatusCode.NOT_FOUND.code());
            response.setContentLength(0);
            return;
        } catch (IllegalArgumentException e) {
            String message = String.format("Got more than 1 item from Nitro for pid %s", pid);
            log.error(message, e);
            telescope.reportFailedEvent(message, itemsWithPayload);
            response.setStatus(HttpStatusCode.SERVER_ERROR.code());
            response.setContentLength(message.length());
            response.getWriter().write(message);
            return;
        }

        createOrUpdateItem(response, pid, itemWithPayload, telescope);
    }

    private Optional<ModelWithPayload<Channel>> fetchService(String sid) {
        try {
            for (ModelWithPayload<Channel> candidateWithPayload : channelAdapter.fetchServices()) {
                for (Alias alias : candidateWithPayload.getModel().getAliases()) {
                    if ("bbc:service:sid".equals(alias.getNamespace())
                            && sid.equals(alias.getValue())) {
                        return Optional.of(candidateWithPayload);
                    }
                }
            }
            return Optional.empty();
        } catch (GlycerinException e) {
            throw Throwables.propagate(e);
        }
    }

    private Optional<ModelWithPayload<Channel>> fetchMasterbrand(String mid) {
        try {
            for (ModelWithPayload<Channel> candidateWithPayload : channelAdapter.fetchMasterbrands()) {
                for (Alias alias : candidateWithPayload.getModel().getAliases()) {
                    if ("bbc:masterbrand:mid".equals(alias.getNamespace())
                            && mid.equals(alias.getValue())) {
                        return Optional.of(candidateWithPayload);
                    }
                }
            }
            return Optional.empty();
        } catch (GlycerinException e) {
            throw Throwables.propagate(e);
        }
    }

    private void createOrUpdateItem(
            HttpServletResponse response,
            String pid,
            ModelWithPayload<Item> itemWithPayload,
            OwlTelescopeReporter telescope
    ) throws IOException {

        updateBrand(response, pid, itemWithPayload, telescope);
        updateSeries(response, pid, itemWithPayload, telescope);

        Item item = itemWithPayload.getModel();

        contentWriter.createOrUpdate(item);
        if (item.getId() != null) {
            telescope.reportSuccessfulEvent(
                    item.getId(),
                    item.getAliases(),
                    EntityType.ITEM,
                    itemWithPayload.getPayload()
            );
        } else {
            telescope.reportFailedEvent(
                    "There was an error while trying to write this Item to Atlas. Item pid=" + pid,
                    EntityType.ITEM,
                    itemWithPayload.getPayload()
            );
        }
        response.setStatus(HttpStatusCode.ACCEPTED.code());

    }

    private void updateSeries(
            HttpServletResponse response,
            String pid,
            ModelWithPayload<Item> itemWithPayload,
            OwlTelescopeReporter telescope
    ) throws IOException {
        Item item = itemWithPayload.getModel();
        if (!(item instanceof Episode)) {
            return;
        }

        ParentRef seriesRef = ((Episode) item).getSeriesRef();
        if (seriesRef == null) {
            /* this is theoretically possible, there's episodes that are part of a brand but don't,
               e.g., have a strong ordering, etc. See NitroEpisodeExtractor#isBrandSeriesEpisode
               vs. NitroEpisodeExtractor#isBrandEpisode
             */
            return;
        }

        String seriesPid = BbcFeeds.pidFrom(seriesRef.getUri());
        PidReference seriesPidRef = new PidReference();
        seriesPidRef.setPid(seriesPid);
        seriesPidRef.setHref(seriesRef.getUri());
        seriesPidRef.setResultType("series");

        try {
            ImmutableSet<ModelWithPayload<Series>> seriesWithPayload;
            seriesWithPayload = contentAdapter.fetchSeries(ImmutableList.of(seriesPidRef));
            ModelWithPayload<Series> seryWithPayload = Iterables.getOnlyElement(seriesWithPayload);
            Series series = seryWithPayload.getModel();
            contentWriter.createOrUpdate(series);
            if (series.getId() != null) {
                telescope.reportSuccessfulEvent(
                        series.getId(),
                        series.getAliases(),
                        EntityType.SERIES,
                        seryWithPayload.getPayload(), itemWithPayload.getPayload());
            } else {
                telescope.reportFailedEvent(
                        "There was an error while trying to write this Series to Atlas. "
                        + " seriesPid=" + seriesPid + " itemPid=" + pid,
                        EntityType.SERIES,
                        itemWithPayload.getPayload(), seryWithPayload.getPayload());
            }
        } catch (NitroException e) {
            log.error("Failed to get Nitro parent item {}", pid, e);
            writeServerErrorWithStack(response, e);
            telescope.reportFailedEvent(
                    "There was an error while trying to write this Series to Atlas. "
                    + " seriesPid=" + seriesPid + " itemPid=" + pid,
                    EntityType.SERIES,
                    itemWithPayload.getPayload());
        }
    }

    private void updateBrand(
            HttpServletResponse response,
            String pid,
            ModelWithPayload<Item> itemWithPayload,
            OwlTelescopeReporter telescope
    ) throws IOException {
        Item item = itemWithPayload.getModel();
        if (item.getContainer() == null) {
            return;
        }

        ParentRef parentRef = item.getContainer();
        String parentPid = BbcFeeds.pidFrom(parentRef.getUri());
        PidReference parentPidRef = new PidReference();
        parentPidRef.setPid(parentPid);
        parentPidRef.setHref(parentRef.getUri());
        parentPidRef.setResultType("brand");

        try {
            ImmutableSet<ModelWithPayload<Brand>> brandsWithPayload =
                    contentAdapter.fetchBrands(ImmutableList.of(parentPidRef));
            /* handle The Curious Case of Top Level Series (In The Night). The container can
               be a Series, not just a brand, in which case it won't hit the early return
               guard, but it will also obviously not return any brands for the above query.
               That's by design, as Nitro's data model is pretty flexible and allows cases like
               these.
            */
            if (!brandsWithPayload.isEmpty()) {
                ModelWithPayload<Brand> brandWithPayload = Iterables.getOnlyElement(brandsWithPayload);
                Brand brand = brandWithPayload.getModel();
                contentWriter.createOrUpdate(brand);
                if (brand.getId() != null) {
                    telescope.reportSuccessfulEvent(
                            brand.getId(),
                            brand.getAliases(),
                            EntityType.BRAND,
                            brandWithPayload.getPayload(), itemWithPayload.getPayload()
                    );
                } else {
                    telescope.reportFailedEvent(
                            "There was an error while trying to write this Brand to Atlas."
                            + " brandPid=" + parentPid + " itemPid=" + pid,
                            EntityType.BRAND,
                            brandWithPayload.getPayload(), itemWithPayload.getPayload()
                    );
                }
            }

        } catch (NitroException e) {
            log.error("Failed to get Nitro parent item {}", pid, e);
            telescope.reportFailedEvent(
                    "There was an error while trying to write this Brand to Atlas. "
                    + " brandPid=" + parentPid + " itemPid=" + pid,
                    EntityType.BRAND,
                    itemWithPayload.getPayload()
            );
            writeServerErrorWithStack(response, e);
        }
    }

    private void writeServerErrorWithStack(HttpServletResponse response, Exception e)
            throws IOException {
        String stack = Throwables.getStackTraceAsString(e);
        response.setStatus(HttpStatusCode.SERVER_ERROR.code());
        response.setContentLength(stack.length());
        response.getWriter().write(stack);
    }
}
