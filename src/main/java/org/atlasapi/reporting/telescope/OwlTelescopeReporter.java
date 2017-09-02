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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * startReporting, then report various events, and finally endReporting. If you do stuff in the
 * wrong order they will silently fail (log errors only). If the proxy fails to connect to telescope
 * it will silently fail (i.e. it will pretend to be reporting, but will report nothing).
 */
public class OwlTelescopeReporter extends TelescopeReporter {

    private static final Logger log = LoggerFactory.getLogger(OwlTelescopeReporter.class);

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(MapperFeature.AUTO_DETECT_GETTERS, false)
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
            .setVisibility(PropertyAccessor.CREATOR, JsonAutoDetect.Visibility.NONE)
            .setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE)
            .setVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.NONE)
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            //add the same mixin to every class that suffers from circular references
            .addMixIn(Iterable.class, PreventCircularReferences.class);

    private final Event.Type eventType;

    protected OwlTelescopeReporter(TelescopeReporterName reporterName, Event.Type eventType) {
        super(reporterName, Configurer.get("telescope.environment").get(), Configurer.get("telescope.host").get());
        this.eventType = eventType;
    }

    public static OwlTelescopeReporter create(TelescopeReporterName reporterName, Event.Type eventType) {
       return new OwlTelescopeReporter(reporterName, eventType);
    }

    private void reportSuccessfulEventGeneric(
            String atlasItemId,
            List<Alias> aliases,
            String warningMsg,
            String payload
    ) {
        if (!isStarted()) {
            logError("It was attempted to report atlasItem={}, but the telescope client was not started.", atlasItemId);
            return;
        }

        EntityState.Builder entityState = EntityState.builder()
                .withAtlasId(atlasItemId)
                .withRaw(payload)
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

    public void reportSuccessfulEvent(
            String atlasItemId,
            List<Alias> aliases,
            Object... objectToSerialise
    ){
        reportSuccessfulEventGeneric(
                atlasItemId,
                aliases,
                null,
                serialize(objectToSerialise)
        );
    }

    public void reportSuccessfulEvent(
            long dbId,Set<org.atlasapi.media.entity.Alias> aliases,
            Object... objectToSerialise
    ) {
        reportSuccessfulEvent(
                encode(dbId),
                TelescopeUtilityMethodsAtlas.getAliases(aliases),
                objectToSerialise
        );
    }

    public void reportSuccessfulEventWithWarning(
            String atlasItemId,
            String warningMsg,
            Object... objectToSerialise
    ) {
        reportSuccessfulEventGeneric(
                atlasItemId,
                null,
                warningMsg,
                serialize(objectToSerialise)
        );
    }

    public void reportFailedEvent(String errorMsg, Object... objectToSerialise) {
        if (!isStarted()) {
            logError("It was attempted to report an error to telescope, but the client has not been started.");
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
            log.warn("An error was reported to telescope after the telescope client (taskId={}) has finished reporting.", getTaskId());
        }
    }

    public static String serialize(Object... objectsToSerialise) {

        StringBuilder sb = new StringBuilder().append("{");
        int i = 1; //to differentiate different objects of the same class.
        for (Object o : objectsToSerialise) { //one by one, so what can be serialized will be serialized.
            if (i != 1) {
                sb.append(',');
            }
            sb.append("\"Payload-").append(i).append("-").append(o.getClass().getSimpleName()).append("\":");
            try {
                sb.append(objectMapper.writeValueAsString(o));
            } catch (JsonProcessingException e) {
                sb.append("{\"objectMapper\": \"Couldn't convert the given object to a JSON string. (" +
                          StringEscapeUtils.escapeJava(e.getMessage()) + ")\"}" );
            }
            i++;
        }
        sb.append("}");
        return sb.toString();
    }

    //this is used as mixin to object mapper. It appends the following field to be used as an identifying id for objects.
    @JsonIdentityInfo(generator=ObjectIdGenerators.IntSequenceGenerator.class, property="@PreventCircularReferencesId")
    interface PreventCircularReferences { }

    // helper method so the log doesn't get spammed if telescope reporting has failed
    private transient boolean errorLogged = false;
    private void logError(String msg, Object... o) {
        if (errorLogged) {
            log.debug(msg, o);
        } else {
            log.error(msg, o);
            errorLogged = true;
        }
    }

}
