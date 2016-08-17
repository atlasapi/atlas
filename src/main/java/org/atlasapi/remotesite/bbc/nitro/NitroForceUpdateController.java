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

import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
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
            return;
        } catch (NitroException e) {
            log.error("Failed to get Nitro item {}", pid, e);
            writeServerErrorWithStack(response, e);
            return;
        }

        createOrUpdateItem(response, pid, item);
    }

    @RequestMapping(value = "/system/bbc/nitro/update/{type}/{id}", method = RequestMethod.POST)
    public void updatePidFromNitro(
            HttpServletResponse response,
            @PathVariable("type") String type,
            @PathVariable("id") String id
    ) throws IOException {
        try {
            switch (type) {
            case "content":
                forceUpdateContent(response, id);
                break;
            case "service":
                forceUpdateChannel(response, this::fetchService, id);
                break;
            case "masterbrand":
                forceUpdateChannel(response, this::fetchMasterbrand, id);
                break;
            default:
                throw new IllegalArgumentException(String.format("Bad type %s", type));
            }
        } catch (IOException e) {
            log.error("Failed to get Nitro thing {}", id, e);
            writeServerErrorWithStack(response, e);
        }
    }

    private void forceUpdateChannel(
            HttpServletResponse response,
            Function<String, Optional<Channel>> channelResolve,
            String id
    ) throws IOException {
        Optional<Channel> channel = channelResolve.apply(id);
        if (!channel.isPresent()) {
            log.error("No items found in Nitro for id {}", id);
            response.setStatus(HttpStatusCode.NOT_FOUND.code());
            response.setContentLength(0);
        } else {
            channelWriter.createOrUpdate(channel.get());
            response.setStatus(HttpStatusCode.ACCEPTED.code());
        }
    }

    private void forceUpdateContent(HttpServletResponse response, String id)
            throws IOException {
        Item item;
        try {
            Iterable<List<Item>> items = contentAdapter
                    .fetchEpisodes(
                            ProgrammesQuery.builder()
                                    .withPid(id)
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
        } catch (NoSuchElementException e) {
            log.error("No items found in Nitro for pid {}", id);
            response.setStatus(HttpStatusCode.NOT_FOUND.code());
            response.setContentLength(0);
            return;
        } catch (NitroException e) {
            log.error("Failed to get Nitro item {}", id, e);
            writeServerErrorWithStack(response, e);
            return;
        }

        createOrUpdateItem(response, id, item);
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
            Item item
    ) throws IOException {
        updateBrand(response, pid, item);
        updateSeries(response, pid, item);

        try {
            contentWriter.createOrUpdate(item);
            response.setStatus(HttpStatusCode.ACCEPTED.code());
        } catch (IllegalArgumentException e) {
            String message = String.format("Got more than 1 item from Nitro for pid %s", pid);
            log.error(message);
            response.setStatus(HttpStatusCode.SERVER_ERROR.code());
            response.setContentLength(message.length());
            response.getWriter().write(message);
        }
    }

    private void updateSeries(
            HttpServletResponse response,
            String pid,
            Item item
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
            ImmutableSet<Series> series = contentAdapter.fetchSeries(
                    ImmutableList.of(seriesPidRef)
            );
            contentWriter.createOrUpdate(Iterables.getOnlyElement(series));
        } catch (NitroException e) {
            log.error("Failed to get Nitro parent item {}", pid, e);
            writeServerErrorWithStack(response, e);
        }
    }

    private void updateBrand(
            HttpServletResponse response,
            String pid,
            Item item
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
                contentWriter.createOrUpdate(Iterables.getOnlyElement(brand));
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