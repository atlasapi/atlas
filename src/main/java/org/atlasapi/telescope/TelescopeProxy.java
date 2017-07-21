package org.atlasapi.telescope;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import com.metabroadcast.columbus.telescope.api.Alias;
import com.metabroadcast.columbus.telescope.api.EntityState;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.api.Process;
import com.metabroadcast.columbus.telescope.api.Task;
import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;
import com.metabroadcast.columbus.telescope.client.TelescopeClientImpl;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.media.MimeType;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import telescope_api_shaded.com.fasterxml.jackson.core.JsonProcessingException;
import telescope_api_shaded.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * To use this class get a TelescopeProxy object through {@link TelescopeFactory}, then
 * startReporting, then report various events, and finally endReporting. If you do stuff in the
 * wrong order they will silently fail (log errors only). If the proxy fails to connect to telescope
 * it will silently fail (i.e. it will pretend to be reporting, but will report nothing).
 */
public class TelescopeProxy {

    private static final Logger log = LoggerFactory.getLogger(TelescopeProxy.class);

    //check for null before use, as it might fail to initialize
    private IngestTelescopeClientImpl telescopeClient;

    private String taskId;
    private Process process;
    private ObjectMapper objectMapper;
    private boolean startedReporting = false; //safeguard flags
    private boolean stoppedReporting = false;
    private SubstitutionTableNumberCodec idCodec; //used to create atlasIDs

    /**
     * The client always reports to {@link TelescopeFactory#TELESCOPE_HOST}
     */
    TelescopeProxy(Process process) {
        this.process = process;

        //the telescope client might fail to initialize, in which case it will remain null,
        // and thus and we'll have to check for that in further operations.
        TelescopeClientImpl client = TelescopeClientImpl.create(TelescopeFactory.TELESCOPE_HOST);
        if (client == null) { //precaution, not sure if it can actually happen.
            log.error(
                    "Could not get a TelescopeClientImpl object with the given TELESCOPE_HOST={}",
                    TelescopeFactory.TELESCOPE_HOST
            );
            log.error(
                    "This telescope proxy will not report to telescope, and will not print any further messages.");
        } else {
            this.telescopeClient = IngestTelescopeClientImpl.create(client);
            this.objectMapper = new ObjectMapper();
        }
    }

    /**
     * Make the telescope aware that a new process has started reporting.
     *
     * @return Returns true on success, and false on failure.
     */
    public boolean startReporting() {
        //do we have a telescope client?
        if (!initialized()) {
            return false;
        }
        //make sure we have not already done that
        if (startedReporting) {
            log.warn(
                    "Someone tried to start a telescope report through a proxy that had already started reporting.");
            return false;
        }

        Task task = telescopeClient.startIngest(process);
        if (task.getId().isPresent()) {
            taskId = task.getId().get();
            startedReporting = true;
            log.debug("Started reporting to Telescope, taskId={}", taskId);
            return true;
        } else {
            //this log might be meaningless, because I might not be understanding under
            // which circumstances this id might be null.
            log.error("Reporting a Process to telescope did not respond with a taskId");
            return false;
        }
    }

    public void reportSuccessfulEvent(String atlasItemId, List<Alias> aliases,
            Object objectToSerialise) {
        if (!allowedToReport()) {
            return;
        }
        try {
            Event reportEvent = Event.builder()
                    .withStatus(Event.Status.SUCCESS)
                    .withType(Event.Type.INGEST)
                    .withEntityState(EntityState.builder()
                            .withAtlasId(atlasItemId)
                            .withRemoteIds(aliases)
                            .withRaw(objectMapper.writeValueAsString(objectToSerialise))
                            .withRawMime(MimeType.APPLICATION_JSON.toString())
                            .build()
                    )
                    .withTaskId(taskId)
                    .withTimestamp(LocalDateTime.now())
                    .build();

            telescopeClient.createEvents(ImmutableList.of(reportEvent));

            log.debug(
                    "Reported successfully event with taskId={}, eventId={}",
                    taskId,
                    reportEvent.getId().orElse("null")
            );
        } catch (JsonProcessingException e) {
            log.error("Couldn't convert the given object to a JSON string.", e);
        }
    }

    //convenience method for the most common reporting Format
    public void reportSuccessfulEvent(
            long dbId,
            Set<org.atlasapi.media.entity.Alias> aliases,
            Object objectToSerialise) {

        reportSuccessfulEvent(
                encode(dbId),
                TelescopeUtilityMethods.getAliases(aliases),
                objectToSerialise
        );
    }

    public void reportFailedEventWithWarning(String warningMsg, Object objectToSerialise) {
        if (!allowedToReport()) {
            return;
        }
        try {
            Event reportEvent = Event.builder()
                    .withStatus(Event.Status.FAILURE)
                    .withType(Event.Type.INGEST)
                    .withEntityState(EntityState.builder()
                            .withWarning(warningMsg)
                            .withRaw(objectMapper.writeValueAsString(objectToSerialise))
                            .withRawMime(MimeType.APPLICATION_JSON.toString())
                            .build()
                    )
                    .withTaskId(taskId)
                    .withTimestamp(LocalDateTime.now())
                    .build();
            telescopeClient.createEvents(ImmutableList.of(reportEvent));

            log.debug(
                    "Reported successfully a FAILED event, taskId={}, warning={}",
                    taskId,
                    warningMsg
            );
        } catch (JsonProcessingException e) {
            log.error("Couldn't convert the given object to a JSON string.", e);
        }
    }

    public void reportFailedEventWithError(String errorMsg, Object objectToSerialise) {
        if (!allowedToReport()) {
            return;
        }
        try {
            Event reportEvent = Event.builder()
                    .withStatus(Event.Status.FAILURE)
                    .withType(Event.Type.INGEST)
                    .withEntityState(EntityState.builder()
                            .withError(errorMsg)
                            .withRaw(objectMapper.writeValueAsString(objectToSerialise))
                            .withRawMime(MimeType.APPLICATION_JSON.toString())
                            .build()
                    )
                    .withTaskId(taskId)
                    .withTimestamp(LocalDateTime.now())
                    .build();
            telescopeClient.createEvents(ImmutableList.of(reportEvent));

            log.debug(
                    "Reported successfully a FAILED event with taskId={}, error={}",
                    taskId,
                    errorMsg
            );
        } catch (JsonProcessingException e) {
            log.error("Couldn't convert the given object to a JSON string.", e);
        }
    }

    /**
     * Let telescope know we are finished reporting through this proxy. Once finished this object is
     * useless.
     */
    public void endReporting() {
        if (!initialized()) {
            return;
        }
        if (startedReporting) {
            telescopeClient.endIngest(taskId);
            stoppedReporting = true;
            log.debug("Finished reporting to Telescope, taskId={}", taskId);
        } else {
            log.warn("Someone tried to stop a telescope report that has never started");
        }
    }

    //To allow for better logging down the line
    public String getTaskId() {
        return taskId;
    }

    public String getIngesterName() {
        return this.process.getKey();
    }

    //======= HELPER METHODS ========
    private boolean allowedToReport() {
        if (!initialized()) {
            return false;
        }
        if (!startedReporting) {
            log.warn(
                    "Someone tried to report a telescope event before the process has started reporting");
            return false;
        }
        if (stoppedReporting) {
            log.warn(
                    "Someone tried to report a telescope event after the process finished reporting");
            return false;
        }
        return true;
    }

    private boolean initialized() {
        return (telescopeClient != null);
    }

    // This allows us to work with db id's instead of atlas ids,
    // without forcing each caller to create his own encoder.
    // This is here and not in the utility class so we dont recreate the codec object all the time.
    private String encode(long id) {
        //lazy initialize
        if (this.idCodec == null) {
            this.idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
        }

        return idCodec.encode(BigInteger.valueOf(id));
    }

}
