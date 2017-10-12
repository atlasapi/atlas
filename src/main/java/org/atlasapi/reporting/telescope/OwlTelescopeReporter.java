package org.atlasapi.reporting.telescope;

import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.atlasapi.media.entity.Identified;
import org.atlasapi.remotesite.amazonunbox.TelescopeReporterHelperMethods;

import com.metabroadcast.columbus.telescope.api.Alias;
import com.metabroadcast.columbus.telescope.api.EntityState;
import com.metabroadcast.columbus.telescope.api.Environment;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.client.EntityType;
import com.metabroadcast.columbus.telescope.client.TelescopeClientImpl;
import com.metabroadcast.columbus.telescope.client.TelescopeReporter;
import com.metabroadcast.columbus.telescope.client.TelescopeReporterName;
//import com.metabroadcast.columbus.telescope.client.http.TelescopeReporterHelperMethods;
import com.metabroadcast.common.media.MimeType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * startReporting, then report various events, and finally endReporting. If you do stuff in the
 * wrong order they will silently fail (log errors only). If the proxy fails to connect to telescope
 * it will silently fail (i.e. it will pretend to be reporting, but will report nothing).
 */
public class OwlTelescopeReporter extends TelescopeReporter {

    private static final Logger log = LoggerFactory.getLogger(OwlTelescopeReporter.class);



    private final Event.Type eventType;

    protected OwlTelescopeReporter(
            TelescopeReporterName reporterName,
            Event.Type eventType,
            Environment environment,
            TelescopeClientImpl client) {
        
        super(reporterName, environment, client);
        this.eventType = eventType;
    }

    public void reportSuccessfulEvent(
            long dbId,Set<org.atlasapi.media.entity.Alias> aliases,
            EntityType entityType,
            Object... objectToSerialise
    ) {
        reportSuccessfulEventGeneric(
                encode(dbId),
                OwlTelescopeUtilityMethodsAtlas.getAliases(aliases),
                entityType,
                null,
                TelescopeReporterHelperMethods.serialize(objectToSerialise)
        );
    }

    /**
     * autoTypeDiscovery means that it will attempt to automatically infer the entityType based on
     * the class of autoTypeDiscoveryObject.
     */
    public <T extends Identified> void reportSuccessfulEvent(
            long dbId,Set<org.atlasapi.media.entity.Alias> aliases,
            T autoTypeDiscoveryObject,
            Object... objectToSerialise) {

        reportSuccessfulEventGeneric(
                encode(dbId),
                OwlTelescopeUtilityMethodsAtlas.getAliases(aliases),
                OwlTelescopeUtilityMethodsAtlas.getEntityTypeFor(autoTypeDiscoveryObject),
                null,
                TelescopeReporterHelperMethods.serialize(objectToSerialise)
        );
    }

    public void reportSuccessfulEventWithWarning(
            String atlasItemId,
            EntityType entityType,
            String warningMsg,
            Object... objectToSerialise
    ) {
        reportSuccessfulEventGeneric(
                atlasItemId,
                null,
                entityType,
                warningMsg,
                TelescopeReporterHelperMethods.serialize(objectToSerialise)
        );
    }

    private void reportSuccessfulEventGeneric(
            @NotNull String atlasItemId,
            @Nullable List<Alias> aliases,
            @Nullable EntityType entityType,
            @Nullable String warningMsg,
            String payload
    ) {
        //fail graciously by reporting nothing, but print a full stack so we know who caused this
        if (atlasItemId == null) {
            log.error(
                    "Cannot report a successful event to telescope",
                    new IllegalArgumentException("No atlasId was given")
            );
            return;
        }

        EntityState.Builder entityState = EntityState.builder()
                .withAtlasId(atlasItemId)
                .withRaw(payload)
                .withRemoteIds(aliases)
                .withType((entityType != null ? entityType.getVerbose() : null))
                .withWarning(warningMsg)
                .withRawMime(MimeType.APPLICATION_JSON.toString());

        Event event = super.getEventBuilder()
                .withType(this.eventType)
                .withStatus(Event.Status.SUCCESS)
                .withEntityState(entityState.build())
                .build();

        reportEvent(event);
    }

    public void reportFailedEvent(String errorMsg, Object... objectToSerialise){
        reportFailedEvent(errorMsg, (EntityType)null, objectToSerialise);
    }

    /**
     * autoTypeDiscovery means that it will attempt to automatically infer the entityType based on
     * the class of autoTypeDiscoveryObject.
     */
    public <T extends Identified> void reportFailedEvent(
            String errorMsg,
            T autoTypeDiscoveryObject,
            Object... objectToSerialise) {

        reportFailedEvent(errorMsg,
                OwlTelescopeUtilityMethodsAtlas.getEntityTypeFor(autoTypeDiscoveryObject),
                objectToSerialise);
    }


    public void reportFailedEvent(
            String errorMsg,
            @Nullable EntityType entityType,
            Object... objectToSerialise) {

        reportFailedEvent(errorMsg, entityType, objectToSerialise);
    }

    public <T extends Identified> void reportFailedEvent(
            long dbId,
            String errorMsg,
            @Nullable EntityType entityType,
            Object... objectToSerialise) {

        Event event = super.getEventBuilder()
                .withType(this.eventType)
                .withStatus(Event.Status.FAILURE)
                .withEntityState(EntityState.builder()
                        .withAtlasId(encode(dbId))
                        .withError(errorMsg)
                        .withType(entityType != null ? entityType.getVerbose() : null)
                        .withRaw(TelescopeReporterHelperMethods.serialize(objectToSerialise))
                        .withRawMime(MimeType.APPLICATION_JSON.toString())
                        .build()
                )

                .build();
        reportEvent(event);
    }

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
