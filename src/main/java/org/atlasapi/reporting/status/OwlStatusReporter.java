package org.atlasapi.reporting.status;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Specialization;

import com.metabroadcast.status.api.EntityRef;
import com.metabroadcast.status.api.PartialStatus;
import com.metabroadcast.status.client.StatusClientWithApp;
import com.metabroadcast.status.client.http.HttpExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OwlStatusReporter {
    private static final Logger log = LoggerFactory.getLogger(OwlStatusReporter.class);
    private final String appId;
    private StatusClientWithApp statusClientWithApp;

    public OwlStatusReporter(HttpExecutor httpExecutor, String appId) {
        this.appId = appId;

        try {
            if(httpExecutor == null) {
                throw new NullPointerException("httpExecutor=" + httpExecutor);
            }
            this.statusClientWithApp = new StatusClientWithApp(httpExecutor, appId);
        } catch (NullPointerException | IllegalArgumentException var10) {
            log.error("Could not create an http executor for the status reporter.\n " +
                    "StatusReporter has protected you from this exception by creating a StatusReporter that will not report to the status service.", var10);
        } catch (Exception var11) {
            log.error("An unknown exception has occurred when creating the http executor\n" +
                    "StatusReporter has protected you from this exception by creating a StatusReporter that will not report to the status service.", var11);
        }
    }

    public void updateStatus(EntityRef.Type type, Described model, PartialStatus partialStatus) {
        //the status service does not care for radio status, so ignore them.
        if (Specialization.RADIO.equals(model.getSpecialization())
            || MediaType.AUDIO.equals(model.getMediaType())) {
            return;
        }
        updateStatus(type, (Identified) model, partialStatus);
    }

    public static final boolean DISABLED = true;
    public void updateStatus(EntityRef.Type type, Identified model, PartialStatus partialStatus) {
        if (DISABLED) {
            return;
        }
        Long id = model.getId();
        if (statusClientWithApp != null) {
            try {
                statusClientWithApp.updateStatus(appId, type, id, partialStatus);
            } catch (Exception e) {
                log.error("An unknown exception occured during .updateStatus " + e + ".\n"+
                        "StatusReporter has protected you from this problem.");
            }
        } else {
            log.error("StatusReporter attempted to update a status while not being initialised properly.\n" +
                    "StatusReporter has protected you from this problem.");
        }
    }
}
