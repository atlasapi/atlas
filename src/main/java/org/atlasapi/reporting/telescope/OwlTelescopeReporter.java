package org.atlasapi.reporting.telescope;

import java.util.List;
import java.util.Set;

import com.metabroadcast.columbus.telescope.api.Alias;
import com.metabroadcast.columbus.telescope.api.EntityState;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.client.TelescopeReporter;
import com.metabroadcast.columbus.telescope.client.TelescopeReporterName;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.properties.Configurer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * startReporting, then report various events, and finally endReporting. If you do stuff in the
 * wrong order they will silently fail (log errors only). If the proxy fails to connect to telescope
 * it will silently fail (i.e. it will pretend to be reporting, but will report nothing).
 */
public class OwlTelescopeReporter extends TelescopeReporter {

    private static final Logger log = LoggerFactory.getLogger(OwlTelescopeReporter.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final Event.Type eventType ;

    protected OwlTelescopeReporter(TelescopeReporterName reporterName, Event.Type eventType) {
        super(reporterName, Configurer.get("telescope.environment").get(), Configurer.get("telescope.host").get());
        this.eventType = eventType;
    }

    public static OwlTelescopeReporter create(TelescopeReporterName reporterName, Event.Type eventType) {
       return new OwlTelescopeReporter(reporterName, eventType);
    }

    private void reportSuccessfulEventGeneric(String atlasItemId, List<Alias> aliases, String warningMsg, Object objectToSerialise){
        if (!isStarted()) {
            log.error("It was attempted to report atlasItem={}, but the telescope client was not started.", atlasItemId);
            return;
        }

        //if this cannot be serialized, convert it to warning.
        String serialized = "";
        try {
            serialized = objectMapper.writeValueAsString(objectToSerialise);
        } catch (JsonProcessingException e) {
            warningMsg = "Object could not be serialized";
        }

        EntityState.Builder entityState = EntityState.builder()
                .withAtlasId(atlasItemId)
                .withRaw(serialized)
                .withRawMime(MimeType.APPLICATION_JSON.toString());
        if (aliases != null) {
            entityState.withRemoteIds(aliases);
        }
        if (warningMsg != null) {
            entityState.withWarning(warningMsg);
        }

        Event event = super.getEventBuilder()
                .withType(this.eventType)
                .withStatus(Event.Status.SUCCESS)
                .withEntityState(entityState.build())
                .build();

        reportEvent(event);

        log.debug("Reported successfully event with taskId={}, eventId={}", getTaskId(), event.getId().orElse("null"));
        if (isFinished()) {
            log.warn("atlasItem={} was reported to telescope client={} after it has finished reporting.", atlasItemId, getTaskId());
        }
    }

    //convenience methods for the most common reporting Formats

    public void reportSuccessfulEvent(String atlasItemId, List<Alias> aliases, Object objectToSerialise){
        reportSuccessfulEventGeneric(atlasItemId, aliases, null, objectToSerialise);
    }

    public void reportSuccessfulEvent(long dbId,Set<org.atlasapi.media.entity.Alias> aliases,Object objectToSerialise) {
        reportSuccessfulEvent(encode(dbId), TelescopeUtilityMethodsAtlas.getAliases(aliases), objectToSerialise);
    }

    public void reportSuccessfulEventWithWarning(String atlasItemId, String warningMsg, Object objectToSerialise) {
        reportSuccessfulEventGeneric(atlasItemId, null, warningMsg, objectToSerialise);
    }

    public void reportFailedEvent(String errorMsg, Object objectToSerialise) {
        if (!isStarted()) {
            log.error("It was attempted to report an error to telescope, but the client was not started.");
             return;
        }
        Event event = super.getEventBuilder()
                .withType(this.eventType)
                .withStatus(Event.Status.FAILURE)
                .withEntityState(EntityState.builder()
                        .withError(errorMsg)
                        .withRaw(serialize(objectToSerialise))
                        .withRawMime(MimeType.APPLICATION_JSON.toString())
                        .build()
                )
                .build();
        reportEvent(event);

        log.debug("Reported successfully a FAILED event with taskId={}, error={}", getTaskId(), errorMsg);
        if (isFinished()) {
            log.warn("An error was reported to telescope after the telescope client={} has finished reporting.", getTaskId());
        }
    }

    public String serialize(Object objectToSerialise) {
        try {
            return objectMapper.writeValueAsString(objectToSerialise);
        } catch (JsonProcessingException e) {
            return "Couldn't convert the given object to a JSON string. " + e.getMessage();
        }
    }

}
