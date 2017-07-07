package org.atlasapi.remotesite.bbc.nitro;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.columbus.telescope.api.Alias;
import com.metabroadcast.columbus.telescope.api.EntityState;
import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;
import com.metabroadcast.columbus.telescope.client.TelescopeClientImpl;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.api.Task;
import com.metabroadcast.columbus.telescope.api.Process;
import com.metabroadcast.common.media.MimeType;
import java.time.LocalDateTime;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import telescope_api_shaded.com.fasterxml.jackson.core.JsonProcessingException;
import telescope_api_shaded.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Author's comment: This class was largely constructed by copying code from RT ingester. It appears, and I might be
 * wrong here, that there is no requirement (or safeguards) for the order that things can happen. Eg one can report that
 * a process is done reporting without ever reporting it has started. To safeguard against this we construct this Proxy
 * to work with a single process.<br>
 * <br>
 * To make this class more friendly and usable, WE ASSUME THAT ONLY ONE PROCESS CAN REPORT PER PROXY. This is possibly
 * wrong, and it could be meaningful to restructure this object, or even make it a singleton so that the whole atlas can
 * report through it. But I don't know cause it is very difficult to reverse engineer.<br>
 * <br>
 * <b>In any case at the moment, the use of this class is to get a TelescopeProxy item, and then startReporting, then
 * report various events, and finally endReporting.</b>
 *
 * @author andreas
 */
public class TelescopeProxy {

    IngestTelescopeClientImpl telescopeClient;

    private static final Logger log = LoggerFactory.getLogger(TelescopeProxy.class);
    private String taskId = null;
    private boolean startedReporting = false; //safeguard flags
    private boolean stoppedReporting = false;
    private ObjectMapper objectMapper;

    public TelescopeProxy() {
        TelescopeClientImpl client = TelescopeClientImpl.create(TelescopeConfiguration.TELESCOPE_HOST);
        this.telescopeClient = IngestTelescopeClientImpl.create(client);

        ObjectMapper om = new ObjectMapper();
    }

    /**
     * Make the telescope aware that a new process has started reporting.
     *
     * @param process
     * @return Returns true on success, and false on failure.
     */
    public boolean startReporting(Process process) {
        //make sure we have not already done that
        if (startedReporting) {
            log.warn("Someone tried to start a telescope report through a proxy that was already initiated.");
            return false;
        }

        Task task = telescopeClient.startIngest(process);
        if (task.getId().isPresent()) {
            taskId = task.getId().get();
            startedReporting = true;
            log.info("Started to report to Telescope (taskId:" + taskId + ")");
            return true;
        } else {
            //this log might be meaningless, beucase I might not be understanding under which circumstances this id
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
        } catch (JsonProcessingException e) {
            log.error("Couldn't convert RT items to a JSON string.", e);
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
     * Let telescope know we are finished reporting through this proxy.
     */
    public void endReporting() {
        if (startedReporting) {
            telescopeClient.endIngest(taskId);
            stoppedReporting = true;
            log.info("Finished reporting to Telescope (taskId:" + taskId + ")");
        } else {
            log.warn("Someone tried to stop a telescope report that has never started");
        }
    }

}
