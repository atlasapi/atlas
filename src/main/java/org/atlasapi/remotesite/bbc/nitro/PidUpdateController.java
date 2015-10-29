package org.atlasapi.remotesite.bbc.nitro;

import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.PEOPLE;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.TITLES;

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

import com.google.common.collect.Iterables;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
import com.metabroadcast.common.http.HttpStatusCode;

@Controller
public class PidUpdateController {

    private static final Logger log = LoggerFactory.getLogger(PidUpdateController.class);

    private final NitroContentAdapter contentAdapter;
    private final ContentWriter contentWriter;

    public PidUpdateController(NitroContentAdapter contentAdapter, ContentWriter contentWriter) {
        this.contentAdapter = contentAdapter;
        this.contentWriter = contentWriter;
    }

    @RequestMapping(value = "/system/bbc/nitro/update/content/{pid}", method = RequestMethod.POST)
    public void updatePidFromNitro(HttpServletResponse response, @PathVariable("pid") String pid) {
        Set<Item> items;
        try {
            items = contentAdapter
                    .fetchEpisodes(ProgrammesQuery.builder()
                            .withPid(pid)
                            .withMixins(TITLES, PEOPLE)
                            .withPageSize(1)
                            .build());
        } catch (NitroException e) {
            log.error("Failed to get Nitro item {}", pid, e);
            return;
        }

        if (items.isEmpty()) {
            response.setStatus(HttpStatusCode.NOT_FOUND.code());
            response.setContentLength(0);
            return;
        }

        try {
            contentWriter.createOrUpdate(Iterables.getOnlyElement(items));
            response.setStatus(HttpStatusCode.ACCEPTED.code());
        } catch (IllegalArgumentException e) {
            log.error("Got more than 1 item from Nitro for pid {}", pid);
            response.setStatus(HttpStatusCode.SERVER_ERROR.code());
        }
    }
}
