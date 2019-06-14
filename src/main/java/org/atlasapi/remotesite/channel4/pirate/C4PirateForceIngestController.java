package org.atlasapi.remotesite.channel4.pirate;

import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.channel4.pirate.model.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Context;
import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class C4PirateForceIngestController {

    private static final Logger log = LoggerFactory.getLogger(C4PirateForceIngestController.class);

    private final ContentWriter contentWriter;
    private final C4PirateItemTransformer transformer;
    private final C4PirateClient c4Client;

    public C4PirateForceIngestController(
            ContentWriter contentWriter,
            C4PirateItemTransformer transformer,
            C4PirateClient c4Client
    ) {
        this.contentWriter = checkNotNull(contentWriter);
        this.transformer = checkNotNull(transformer);
        this.c4Client = checkNotNull(c4Client);
    }

    @RequestMapping(value="/system/update/c4/pirate",method= RequestMethod.POST)
    public void forceIngest(
            @RequestParam("episodeIds") String episodeIds,
            @Context HttpServletResponse response
    ) throws IOException {

        List<Item> items = c4Client.getItems(episodeIds);
        try {
            items.stream()
                    .map(Item::getEditorialInformation)
                    .map(transformer::toEpisodeSeriesBrand)
                    .forEach(eSB -> {

                        if (eSB.getBrand() != null) {
                            contentWriter.createOrUpdate(eSB.getBrand()); // write brand

                            if(eSB.getBrand().getId() != null) {
                                // If it has been written
                                eSB.getSeries().setParent(eSB.getBrand());
                                eSB.getEpisode().setContainer(eSB.getBrand());
                            }
                        }

                        if (eSB.getSeries() != null) {
                            contentWriter.createOrUpdate(eSB.getSeries()); // write series
                            if (eSB.getSeries().getId() != null) {
                                // If it has been written
                                eSB.getEpisode().setSeries(eSB.getSeries());

                                // If a brand failed to write, set as container to write episode
                                if (eSB.getEpisode().getContainer() == null) {
                                    eSB.getEpisode().setContainer(eSB.getSeries());
                                }
                            }
                        }

                        contentWriter.createOrUpdate(eSB.getEpisode()); // write episode
                    });

            response.setStatus(202);

        } catch (Exception e) {
            log.error("Error forcing ingest from C4 pirate feed", e);
            response.sendError(500, "Failed to ingest");
        }
    }
}
