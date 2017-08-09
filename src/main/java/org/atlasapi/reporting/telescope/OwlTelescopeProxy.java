package org.atlasapi.reporting.telescope;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import com.metabroadcast.columbus.telescope.api.Alias;
import com.metabroadcast.columbus.telescope.api.EntityState;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.client.TelescopeReporter;
import com.metabroadcast.columbus.telescope.client.TelescopeReporterName;
import com.metabroadcast.common.media.MimeType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * startReporting, then report various events, and finally endReporting. If you do stuff in the
 * wrong order they will silently fail (log errors only). If the proxy fails to connect to telescope
 * it will silently fail (i.e. it will pretend to be reporting, but will report nothing).
 *
 *  ===============================================================================
 *  IF YOU MAKE CHANGES TO THIS CLASS, MIRROR THEM IN {@link OwlTelescopeProxyMock}
 *  ===============================================================================
 */
public class OwlTelescopeProxy extends TelescopeReporter {

    private static final Logger log = LoggerFactory.getLogger(OwlTelescopeProxy.class);

    private ObjectMapper objectMapper;

    private Event.Type eventType ;

    protected OwlTelescopeProxy(TelescopeReporterName reporterName, Event.Type eventType) {
        super(reporterName, TelescopeConfiguration.ENVIRONMENT, TelescopeConfiguration.TELESCOPE_HOST);
        this.objectMapper = new ObjectMapper();
        this.eventType = eventType;
    }

    public static OwlTelescopeProxy create(TelescopeReporterName reporterName, Event.Type eventType) {
       return new OwlTelescopeProxy(reporterName, eventType);
    }

    public void reportSuccessfulEvent(
            String atlasItemId, List<Alias> aliases, Object objectToSerialise) {

        if (!isStarted()) {
            log.error(
                    "It was attempted to report atlasItem={}, but the telescope client was not started.",
                    atlasItemId
            );
            return;
        }
        if (isFinished()) {
            log.warn(
                    "atlasItem={} was reported to telescope client={} after it has finished reporting.",
                    atlasItemId,
                    getTaskId()
            );
        }
        try {
            Event event = Event.builder()
                    .withStatus(Event.Status.SUCCESS)
                    .withType(this.eventType)
                    .withEntityState(EntityState.builder()
                            .withAtlasId(atlasItemId)
                            .withRemoteIds(aliases)
                            .withRaw(objectMapper.writeValueAsString(objectToSerialise))
                            .withRawMime(MimeType.APPLICATION_JSON.toString())
                            .build()
                    )
                    .withTaskId(getTaskId())
                    .withTimestamp(LocalDateTime.now())
                    .build();

            reportEvent(event);

            log.debug(
                    "Reported successfully event with taskId={}, eventId={}",
                    getTaskId(),
                    event.getId().orElse("null")
            );
        } catch (JsonProcessingException e) {
            log.error(
                    "Couldn't convert the given object={} to a JSON string.",
                    objectToSerialise, e
            );
        }
    }

    //convenience method for the most common reporting Format
    public void reportSuccessfulEvent(
            long dbId,
            Set<org.atlasapi.media.entity.Alias> aliases,
            Object objectToSerialise) {

        reportSuccessfulEvent(
                encode(dbId),
                TelescopeUtilityMethodsAtlas.getAliases(aliases),
                objectToSerialise
        );
    }

    public void reportFailedEventWithWarning(
            String atlasItemId, String warningMsg, Object objectToSerialise) {

        if (!isStarted()) {
            log.error(
                    "It was attempted to report atlasItem={}, but the telescope client was not started.",
                    atlasItemId
            );
            return;
        }
        if (isFinished()) {
            log.warn(
                    "atlasItem={} was reported to telescope client={} after it has finished reporting.",
                    atlasItemId,
                    getTaskId()
            );
        }
        try {
            Event event = Event.builder()
                    .withStatus(Event.Status.FAILURE)
                    .withType(this.eventType)
                    .withEntityState(EntityState.builder()
                            .withAtlasId(atlasItemId)
                            .withWarning(warningMsg)
                            .withRaw(objectMapper.writeValueAsString(objectToSerialise))
                            .withRawMime(MimeType.APPLICATION_JSON.toString())
                            .build()
                    )
                    .withTaskId(getTaskId())
                    .withTimestamp(LocalDateTime.now())
                    .build();
            reportEvent(event);

            log.debug(
                    "Reported successfully a FAILED event, taskId={}, warning={}",
                    getTaskId(),
                    warningMsg
            );
        } catch (JsonProcessingException e) {
            log.error("Couldn't convert the given object to a JSON string.", e);
        }
    }

    public void reportFailedEventWithError(String errorMsg, Object objectToSerialise) {
            if (!isStarted()) {
                log.error(
                        "It was attempted to report an error to telescope, but the client was not started."
                );
                return;
            }
            if (isFinished()) {
                log.warn(
                        "An error was reported to telescope after the telescope client={} has finished reporting.",
                        getTaskId()
                );
            }
        try {
            Event event = Event.builder()
                    .withStatus(Event.Status.FAILURE)
                    .withType(this.eventType)
                    .withEntityState(EntityState.builder()
                            .withError(errorMsg)
                            .withRaw(objectMapper.writeValueAsString(objectToSerialise))
                            .withRawMime(MimeType.APPLICATION_JSON.toString())
                            .build()
                    )
                    .withTaskId(getTaskId())
                    .withTimestamp(LocalDateTime.now())
                    .build();
            reportEvent(event);

            log.debug(
                    "Reported successfully a FAILED event with taskId={}, error={}",
                    getTaskId(),
                    errorMsg
            );
        } catch (JsonProcessingException e) {
            log.error("Couldn't convert the given object to a JSON string.", e);
        }
    }

}
