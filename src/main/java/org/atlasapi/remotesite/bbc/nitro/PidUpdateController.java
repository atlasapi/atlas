package org.atlasapi.remotesite.bbc.nitro;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.ANCESTOR_TITLES;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.CONTRIBUTIONS;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.IMAGES;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.GENRE_GROUPINGS;

import java.io.IOException;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
import com.metabroadcast.common.http.HttpStatusCode;

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
        Set<Item> items;
        try {
            items = contentAdapter
                    .fetchEpisodes(ProgrammesQuery.builder()
                            .withPid(pid)
                            .withMixins(ANCESTOR_TITLES, CONTRIBUTIONS, IMAGES, GENRE_GROUPINGS)
                            .withPageSize(1)
                            .build());
        } catch (NitroException e) {
            log.error("Failed to get Nitro item {}", pid, e);
            String stack = Throwables.getStackTraceAsString(e);
            response.setStatus(HttpStatusCode.SERVER_ERROR.code());
            response.setContentLength(stack.length());
            response.getWriter().write(stack);
            return;
        }

        if (items.isEmpty()) {
            log.error("No items found in Nitro for pid {}", pid);
            response.setStatus(HttpStatusCode.NOT_FOUND.code());
            response.setContentLength(0);
            return;
        }

        try {
            contentWriter.createOrUpdate(Iterables.getOnlyElement(items));
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
