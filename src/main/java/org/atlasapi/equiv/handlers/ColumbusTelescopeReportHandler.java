package org.atlasapi.equiv.handlers;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.columbus.telescope.api.Alias;
import com.metabroadcast.columbus.telescope.api.EntityState;
import com.metabroadcast.columbus.telescope.api.Environment;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.api.Ingester;
import com.metabroadcast.columbus.telescope.api.Task;
import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;
import com.metabroadcast.columbus.telescope.client.TelescopeClient;
import com.metabroadcast.columbus.telescope.client.TelescopeClientImpl;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumbusTelescopeReportHandler <T extends Item>
        implements EquivalenceResultHandler<Item>  {

    private final Logger logger = LoggerFactory.getLogger(ColumbusTelescopeReportHandler.class);
    private final IngestTelescopeClientImpl telescopeClient;
    private final ObjectMapper mapper;
    private final SubstitutionTableNumberCodec codec;

    public ColumbusTelescopeReportHandler() {
        TelescopeClient client = TelescopeClientImpl.create("columbus-telescope-stage.mbst.tv");
        telescopeClient = IngestTelescopeClientImpl.create(client);
        mapper = new ObjectMapper();
        codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    }

    @Override
    public void handle(EquivalenceResult<Item> result) {
        Ingester ingester = createIngester();
        Task task = telescopeClient.startIngest(ingester);

        Optional<String> ingestId = task.getId();
        if (!ingestId.isPresent()) {
            throw new IllegalArgumentException("No Task Id received. ");
        }

        List<Alias> aliases = createAliases();
        String rawData = getRawObject(result.strongEquivalences());

        EntityState entityState = createEventState(result, aliases, rawData);

        Event event = createEvent(ingestId, entityState);

        telescopeClient.createEvents(ImmutableList.of(event));
        telescopeClient.endIngest(ingestId.get());
    }

    private EntityState createEventState(EquivalenceResult<Item> result, List<Alias> aliases,
            String rawData) {
        return EntityState.builder()
                    .withAtlasId(codec.encode(BigInteger.valueOf(result.subject().getId())))
                    .withRaw(rawData)
                    .withRemoteIds(aliases)
                    .build();
    }

    private Event createEvent(Optional<String> ingestId, EntityState entityState) {
        return Event.builder()
                    .withStatus(Event.Status.SUCCESS)
                    .withType(Event.Type.INGEST)
                    .withEntityState(entityState)
                    .withTaskId(ingestId.get())
                    .withTimestamp(LocalDateTime.now())
                    .build();
    }

    // TODO : What if failure?
    //        } else {
    //            EntityState entityState = EntityState.builder()
    //                    .withAtlasId(item.getAtlasId())
    //                    .withRaw(item.getRaw())
    //                    .withRemoteIds(aliases)
    //                    .withError(item.getErrorMessage())
    //                    .build();
    //
    //            Event event = Event.builder()
    //                    .withStatus(Event.Status.FAILURE)
    //                    .withType(Event.Type.INGEST)
    //                    .withEntityState(entityState)
    //                    .withTaskId(ingestId)
    //                    .withTimestamp(item.getTimeStamp())
    //                    .build();
    //            eventList.add(event);
    //        }

    private List<Alias> createAliases() {
        List<Alias> aliases = Lists.newArrayList();
        aliases.add(Alias.create(
                "atlas-deer-equiv:content:id",
                "alias value"
        ));
        return aliases;
    }

    private Ingester createIngester() {
        return Ingester.create(
                    "atlas-deer-equiv",
                    "Atlas Deer Equiv",
                    Environment.STAGE
            );
    }

    public String getRawObject(
            Multimap<Publisher, ScoredCandidate<Item>> publisherScoredCandidateMultimap
    ) {
        List<Map.Entry<Publisher, ScoredCandidate<Item>>> entries = Lists.newArrayList(
                publisherScoredCandidateMultimap.entries().iterator()
        );

        String rawData = "{";
        for (Map.Entry<Publisher, ScoredCandidate<Item>> entry : entries) {
            try {
                rawData = rawData + mapper.writeValueAsString(entry.getKey()) + "\n";
            } catch (JsonProcessingException e) {
                logger.error("Couldn't extract Publisher data to a string +"
                        + "while sending raw data to Columbus Telescope.", e);
            }
            rawData = rawData + ",";
            try {
                rawData = rawData + mapper.writeValueAsString(entry.getValue()) + "\n";
            } catch (JsonProcessingException e) {
                logger.error("Couldn't extract Scored Candidate data to a string +"
                        + "while sending raw data to Columbus Telescope.", e);
            }
            rawData = rawData + "}";
        }
        return rawData;
    }

}
