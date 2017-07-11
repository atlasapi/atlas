package org.atlasapi.telescope;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.columbus.telescope.api.*;
import com.metabroadcast.columbus.telescope.api.Process;
import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;
import com.metabroadcast.columbus.telescope.client.TelescopeClientImpl;
import com.metabroadcast.common.media.MimeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import telescope_api_shaded.com.fasterxml.jackson.core.JsonProcessingException;
import telescope_api_shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.List;
import org.atlasapi.remotesite.bbc.nitro.ChannelDay;

/**
 * In any case at the moment, the use of this class is to simply get a TelescopeProxy item, then startReporting, then
 * report various events, and finally endReporting. We print log warnings for the wrong order of things.
 *
 * @author andreas
 */
public class TelescopeProxy {

    private static final Logger log = LoggerFactory.getLogger(TelescopeProxy.class);

    private IngestTelescopeClientImpl telescopeClient;
    private String taskId = null;
    private Process process;
    private ObjectMapper objectMapper;
    private boolean startedReporting = false; //safeguard flags
    private boolean stoppedReporting = false;

    /**
     * The client always reports to {@link TelescopeConfiguration#TELESCOPE_HOST}
     */
    TelescopeProxy(Process process) {
        this.process = process;
        //get a client
        TelescopeClientImpl client = TelescopeClientImpl.create(TelescopeConfiguration.TELESCOPE_HOST);
        if (client == null) {
            throw new NullPointerException("Could now get a TelescopeClientImpl object with the given TELESCOPE_HOST:" + TelescopeConfiguration.TELESCOPE_HOST);
        }
        this.telescopeClient = IngestTelescopeClientImpl.create(client);

        objectMapper = new ObjectMapper();
    }

    /**
     * Make the telescope aware that a new process has started reporting.
     *
     * @return Returns true on success, and false on failure.
     */
    public boolean startReporting() {
        //make sure we have not already done that
        if (startedReporting) {
            log.warn("Someone tried to start a telescope report through a proxy that was already initiated.");
            return false;
        }

        Task task = telescopeClient.startIngest(process);
        if (task.getId().isPresent()) {
            taskId = task.getId().get();
            startedReporting = true;
            log.info("Started reporting to Telescope (taskId:{})", taskId);
            return true;
        } else {
            //this log might be meaningless, because I might not be understanding under which circumstances this id
            //might be null.
            log.warn("Reporting a Process to telescope did not respond with an ID");
            return false;
        }
    }

    public void reportSuccessfulEvent(String atlasItemId, List<Alias> aliases, Object objectToSerialise) {
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

            log.info("Reported successfully event with taskId {}", reportEvent.getTaskId().get());
        } catch (JsonProcessingException e) {
            log.error("Couldn't parse brand to a JSON string.", e);
        }
    }

    public void reportFailedEvent(String errorMessage, Object objectToSerialise) {
        if (!allowedToReport()) {
            return;
        }
        try {
            Event reportEvent = Event.builder()
                    .withStatus(Event.Status.FAILURE)
                    .withType(Event.Type.INGEST)
                    .withEntityState(EntityState.builder()
                            .withError(errorMessage)
                            .withRaw(objectMapper.writeValueAsString(objectToSerialise))
                            .withRawMime(MimeType.APPLICATION_JSON.toString())
                            .build()
                    )
                    .withTaskId(taskId)
                    .withTimestamp(LocalDateTime.now())
                    .build();
            telescopeClient.createEvents(ImmutableList.of(reportEvent));

            log.info("Reported succeffully FAILED event with taskId {}", reportEvent.getTaskId().get());
        } catch (JsonProcessingException e) {
            log.error("Couldn't convert the given object to a JSON string.", e);
        }
    }

    private boolean allowedToReport() {
        if (!startedReporting) {
            log.warn("Someone tried to report a telescope event before the process has started reporting");
            return false;
        }
        if (stoppedReporting) {
            log.warn("Someone tried to report a telescope event after the process finished reporting");
            return false;
        }
        return true;
    }

    /**
     * Let telescope know we are finished reporting through this proxy. Once finished this object is useless.
     */
    public void endReporting() {
        if (startedReporting) {
            telescopeClient.endIngest(taskId);
            stoppedReporting = true;
            log.info("Finished reporting to Telescope (taskId:)", taskId);
        } else {
            log.warn("Someone tried to stop a telescope report that has never started");
        }
    }

}
