package org.atlasapi.equiv;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.columbus.telescope.api.Alias;
import com.metabroadcast.columbus.telescope.api.EntityState;
import com.metabroadcast.columbus.telescope.api.Environment;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.api.Process;
import com.metabroadcast.columbus.telescope.api.Task;
import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.media.MimeType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumbusTelescopeReporter<T extends Content> {

    private final Logger log = LoggerFactory.getLogger(ColumbusTelescopeReporter.class);
    private final SubstitutionTableNumberCodec codec;
    private final ObjectMapper mapper;
    private final IngestTelescopeClientImpl telescopeClient;

    public ColumbusTelescopeReporter(
            IngestTelescopeClientImpl telescopeClient
    ) {
        this.codec = SubstitutionTableNumberCodec.lowerCaseOnly();
        this.mapper = new ObjectMapper();
        this.telescopeClient = telescopeClient;
    }

    public void reportItem(
            Optional<String> taskId,
            EquivalenceResult<T> result
    ) {
        report(taskId, result.subject().getId(), result.description());
    }

    private void report(
            Optional<String> taskId,
            Long contentId,
            ReadableDescription description
    ) {
        if (!taskId.isPresent()) {
            throw new IllegalArgumentException("No Task ID received from Columbus Telescope.");
        }

        List<Alias> aliases = createAliases();

        EntityState entityState = createEventState(
                contentId,
                aliases,
                description
        );

        Event event = createEvent(taskId, entityState);

        telescopeClient.createEvents(ImmutableList.of(event));
    }

    public EntityState createEventState(
            Long id,
            List<Alias> aliases,
            ReadableDescription description
    ) {
        return EntityState.builder()
                .withAtlasId(codec.encode(BigInteger.valueOf(id)))
                .withRaw(getRawDataDescription(description))
                .withRawMime(MimeType.APPLICATION_JSON.toString())
                .withRemoteIds(aliases)
                .build();
    }

    public Event createEvent(Optional<String> ingestId, EntityState entityState) {
        return Event.builder()
                .withStatus(Event.Status.SUCCESS)
                .withType(Event.Type.EQUIVALENCE)
                .withEntityState(entityState)
                .withTaskId(ingestId.get())
                .withTimestamp(LocalDateTime.now())
                .build();
    }

    public List<Alias> createAliases() {
        List<Alias> aliases = Lists.newArrayList();
        aliases.add(Alias.create(
                "atlas-owl-equiv:content:id",
                "alias value"
        ));
        return aliases;
    }

    public String getRawDataDescription(ReadableDescription description) {
        try {
            return mapper.writeValueAsString(description);
        } catch (JsonProcessingException e) {
            log.error("Couldn't convert equiv result description to a JSON string.", e);
            return e.getMessage();
        }
    }

    public Optional<String> startReporting(
            Publisher publisher,
            String environment
    ) {
        Process ingester = createIngester(publisher.title(), environment);
        Task task = telescopeClient.startIngest(ingester);
        return task.getId();
    }

    public void endReporting(Optional<String> taskId) {
        telescopeClient.endIngest(taskId.get());
    }

    private Process createIngester(String publisher, String environment) {
        return Process.create(
                String.format("atlas-owl-equiv-%s", publisher.toLowerCase()
                        .replace(" ", "-")
                        .replace("\n", "")),
                String.format("Atlas Owl Equiv %s", publisher),
                Environment.valueOf(environment)
        );
    }
}
