package org.atlasapi.remotesite.bbc.nitro;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.bbc.BbcFeeds;

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
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.CONTRIBUTIONS;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.GENRE_GROUPINGS;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.IMAGES;

@Controller
public class PidUpdateController {

    private static final Logger log = LoggerFactory.getLogger(PidUpdateController.class);

    private final NitroContentAdapter contentAdapter;
    private final ContentWriter contentWriter;

    public PidUpdateController(NitroContentAdapter contentAdapter, ContentWriter contentWriter) {
        this.contentAdapter = checkNotNull(contentAdapter);
        this.contentWriter = checkNotNull(contentWriter);
    }

    @RequestMapping(value = "/system/bbc/nitro/update/content/{pid}", method = RequestMethod.POST)
    public void updatePidFromNitro(HttpServletResponse response, @PathVariable("pid") String pid)
            throws IOException {
        Iterable<List<Item>> items;
        try {
            items = contentAdapter
                    .fetchEpisodes(ProgrammesQuery.builder()
                            .withPid(pid)
                            .withMixins(ANCESTOR_TITLES, CONTRIBUTIONS, IMAGES, GENRE_GROUPINGS)
                            .withPageSize(1)
                            .build());
        } catch (NitroException e) {
            log.error("Failed to get Nitro item {}", pid, e);
            writeServerErrorWithStack(response, e);
            return;
        }

        if (Iterables.getOnlyElement(items) == null) {
            log.error("No items found in Nitro for pid {}", pid);
            response.setStatus(HttpStatusCode.NOT_FOUND.code());
            response.setContentLength(0);
            return;
        }

        List<Item> itemsList = Iterables.getOnlyElement(items);
        Iterator<Item> itemIterator = itemsList.iterator();

        while (itemIterator.hasNext()) {
            Item item = itemIterator.next();
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
