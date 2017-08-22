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
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;

import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
import com.metabroadcast.columbus.telescope.api.Event;
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
        Item item;
        OwlTelescopeReporter telescope = OwlTelescopeReporter.create(
                OwlTelescopeReporters.BBC_NITRO_INGEST_API,
                Event.Type.INGEST
        );
        telescope.startReporting();
        try {
            Iterable<List<Item>> itemListIterable = contentAdapter
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

            Iterable<Item> allItems = Iterables.concat(itemListIterable);
            item = Iterables.getOnlyElement(localOrRemoteNitroFetcher.resolveItems(allItems).getAll());
        } catch (NoSuchElementException e) {
            log.error("No items found in Nitro for pid {}", pid);
            response.setStatus(HttpStatusCode.NOT_FOUND.code());
            response.setContentLength(0);
            telescope.reportFailedEvent("No items found in Nitro for pid=" + pid, "");
            telescope.endReporting();
            return;
        } catch (NitroException e) {
            log.error("Failed to get Nitro item {}", pid, e);
            writeServerErrorWithStack(response, e);
            telescope.reportFailedEvent(
                    "Failed to get Nitro item for pid=" + pid + " (" + e.getMessage() + ")", "");
            telescope.endReporting();
            return;
        }

        createOrUpdateItem(response, pid, item, telescope);
        telescope.endReporting();
    }

    @RequestMapping(value = "/system/bbc/nitro/update/{type}/{pid}", method = RequestMethod.POST)
    public void updatePidFromNitro(
            HttpServletResponse response,
            @PathVariable("type") String type,
            @PathVariable("pid") String pid
    ) throws IOException {
        OwlTelescopeReporter telescope = OwlTelescopeReporter.create(
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
        }
        catch (IOException e) {
            log.error("Failed to get Nitro thing {}", pid, e);
            telescope.reportFailedEvent(
                    "Failed to get Nitro item for pid=" + pid + " (" + e.getMessage() + ")", "");
            writeServerErrorWithStack(response, e);
        }
        finally{
            telescope.endReporting();
        }
    }

    private void forceUpdateChannel(
            HttpServletResponse response,
            Function<String, Optional<Channel>> channelResolve,
            String pid,
            OwlTelescopeReporter telescope
    ) throws IOException {
        Optional<Channel> channel = channelResolve.apply(pid);
        if (!channel.isPresent()) {
            log.error("No items found in Nitro for id {}", pid);
            telescope.reportFailedEvent("No items found in Nitro for pid=" + pid, "");
            response.setStatus(HttpStatusCode.NOT_FOUND.code());
            response.setContentLength(0);
        } else {
            channelWriter.createOrUpdate(channel.get());
            response.setStatus(HttpStatusCode.ACCEPTED.code());
            if(channel.get().getId() != null) {
                telescope.reportSuccessfulEvent(
                        channel.get().getId(),
                        channel.get().getAliases(),
                        channel.get());
            } else {
                telescope.reportFailedEvent(
                        "There was an error while trying to write this Channel to Atlas. Channel pid=" + pid,
                        channel.get()
                );
            }
        }
    }

    private void forceUpdateContent(HttpServletResponse response, String pid, OwlTelescopeReporter telescope)
            throws IOException {
        Item item;
        try {
            Iterable<List<Item>> items = contentAdapter
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
            item = Iterables.getOnlyElement(items).get(0);
        } catch (NoSuchElementException | IndexOutOfBoundsException e ) {
            log.error("No items found in Nitro for pid {}", pid);
            telescope.reportFailedEvent("No items found in Nitro for pid=" + pid, "");
            response.setStatus(HttpStatusCode.NOT_FOUND.code());
            response.setContentLength(0);
            return;
        } catch (NitroException e) {
            log.error("Failed to get Nitro item {}", pid, e);
            telescope.reportFailedEvent(
                    "Failed to get Nitro item for pid=" + pid + " (" + e.getMessage() + ")", "");
            writeServerErrorWithStack(response, e);
            return;
        }

        createOrUpdateItem(response, pid, item, telescope);
    }

    private Optional<Channel> fetchService(String sid) {
        try {
            for (Channel candidate : channelAdapter.fetchServices()) {
                for (Alias alias : candidate.getAliases()) {
                    if ("bbc:service:sid".equals(alias.getNamespace())
                            && sid.equals(alias.getValue())) {
                        return Optional.of(candidate);
                    }
                }
            }
            return Optional.empty();
        } catch (GlycerinException e) {
            throw Throwables.propagate(e);
        }
    }

    private Optional<Channel> fetchMasterbrand(String mid) {
        try {
            for (Channel candidate : channelAdapter.fetchMasterbrands()) {
                for (Alias alias : candidate.getAliases()) {
                    if ("bbc:masterbrand:mid".equals(alias.getNamespace())
                            && mid.equals(alias.getValue())) {
                        return Optional.of(candidate);
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
            Item item,
            OwlTelescopeReporter telescope
    ) throws IOException {

        updateBrand(response, pid, item, telescope);
        updateSeries(response, pid, item, telescope);

        try {
            contentWriter.createOrUpdate(item);
            if (item.getId() != null) {
                telescope.reportSuccessfulEvent(item.getId(), item.getAliases(), item);
            } else {
                telescope.reportFailedEvent(
                        "There was an error while trying to write this Item to Atlas. Item pid=" + pid,
                        item);
            }
            response.setStatus(HttpStatusCode.ACCEPTED.code());
        } catch (IllegalArgumentException e) {
            String message = String.format("Got more than 1 item from Nitro for pid %s", pid);
            log.error(message);
            telescope.reportFailedEvent(
                    "Got more than 1 item from Nitro for pid=" + pid,
                    item);
            response.setStatus(HttpStatusCode.SERVER_ERROR.code());
            response.setContentLength(message.length());
            response.getWriter().write(message);
        }
    }

    private void updateSeries(
            HttpServletResponse response,
            String pid,
            Item item,
            OwlTelescopeReporter telescope
    ) throws IOException {
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
            ImmutableSet<Series> series;
            series = contentAdapter.fetchSeries(ImmutableList.of(seriesPidRef));
            Series onlySeries = Iterables.getOnlyElement(series);
            contentWriter.createOrUpdate(onlySeries);
            if (onlySeries.getId() != null) {
                telescope.reportSuccessfulEvent(onlySeries.getId(), onlySeries.getAliases(), item, onlySeries);
            } else {
                telescope.reportFailedEvent(
                        "There was an error while trying to write this Series to Atlas. Series pid=" + seriesPid,
                        item, onlySeries);
            }
        } catch (NitroException e) {
            log.error("Failed to get Nitro parent item {}", pid, e);
            writeServerErrorWithStack(response, e);
        }
    }

    private void updateBrand(
            HttpServletResponse response,
            String pid,
            Item item,
            OwlTelescopeReporter telescope
    ) throws IOException {
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
            ImmutableSet<Brand> brand = contentAdapter.fetchBrands(
                    ImmutableList.of(parentPidRef)
            );

            /* handle The Curious Case of Top Level Series (In The Night). The container can
               be a Series, not just a brand, in which case it won't hit the early return
               guard, but it will also obviously not return any brands for the above query.
               That's by design, as Nitro's data model is pretty flexible and allows cases like
               these.
            */
            if (!brand.isEmpty()) {
                Brand onlyBrand = Iterables.getOnlyElement(brand);
                contentWriter.createOrUpdate(onlyBrand);
                if (onlyBrand.getId() != null) {
                    telescope.reportSuccessfulEvent(
                            onlyBrand.getId(),
                            onlyBrand.getAliases(),
                            item, onlyBrand
                    );
                } else {
                    telescope.reportFailedEvent(
                            "There was an error while trying to write this Brand to Atlas. Brand pid=" + parentPid,
                            item, onlyBrand
                    );
                }
            }

        } catch (NitroException e) {
            log.error("Failed to get Nitro parent item {}", pid, e);
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
