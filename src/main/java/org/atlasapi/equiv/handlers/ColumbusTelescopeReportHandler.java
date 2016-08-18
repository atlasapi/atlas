package org.atlasapi.equiv.handlers;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.media.entity.Item;

import com.metabroadcast.columbus.telescope.api.Alias;
import com.metabroadcast.columbus.telescope.api.EntityState;
import com.metabroadcast.columbus.telescope.api.Environment;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.api.Ingester;
import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.media.MimeType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ColumbusTelescopeReportHandler <T extends Item>
        implements EquivalenceResultHandler<Item>  {

    private final Logger log = LoggerFactory.getLogger(ColumbusTelescopeReportHandler.class);
    private final SubstitutionTableNumberCodec codec;
    private final ObjectMapper mapper;

    public ColumbusTelescopeReportHandler() {
        this.codec = SubstitutionTableNumberCodec.lowerCaseOnly();
        this.mapper = new ObjectMapper();
    }

    @Override
    public void handle(
            EquivalenceResult<Item> result,
            Optional<String> taskId,
            IngestTelescopeClientImpl telescopeClient
    ) {
        if (!taskId.isPresent()) {
            throw new IllegalArgumentException("No Task ID received from Columbus Telescope.");
        }

        List<Alias> aliases = createAliases();

        EntityState entityState = null;
        try {
            entityState = createEventState(
                    result.subject().getId(),
                    aliases,
                    mapper.writeValueAsString(result.description())
            );
        } catch (JsonProcessingException e) {
            log.error("Couldn't convert equiv result description to a JSON string.", e);
        }

        Event event = createEvent(taskId, entityState);

        telescopeClient.createEvents(ImmutableList.of(event));
    }

    private EntityState createEventState(
            Long id,
            List<Alias> aliases,
            String rawData
    ) {
        return EntityState.builder()
                .withAtlasId(codec.encode(BigInteger.valueOf(id)))
                .withRaw(rawData)
                .withRawMime(MimeType.APPLICATION_JSON.toString())
                .withRemoteIds(aliases)
                .build();
    }

    private Event createEvent(Optional<String> ingestId, EntityState entityState) {
        return Event.builder()
                .withStatus(Event.Status.SUCCESS)
                .withType(Event.Type.EQUIVALENCE)
                .withEntityState(entityState)
                .withTaskId(ingestId.get())
                .withTimestamp(LocalDateTime.now())
                .build();
    }

    private List<Alias> createAliases() {
        List<Alias> aliases = Lists.newArrayList();
        aliases.add(Alias.create(
                "atlas-owl-equiv:content:id",
                "alias value"
        ));
        return aliases;
    }
}