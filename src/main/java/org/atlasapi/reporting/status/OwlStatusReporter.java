package org.atlasapi.reporting.status;

import com.metabroadcast.status.api.EntityRef;
import com.metabroadcast.status.api.PartialStatus;
import com.metabroadcast.status.client.StatusClientWithApp;
import com.metabroadcast.status.client.http.HttpExecutor;
import telescope_client_shaded.org.slf4j.Logger;
import telescope_client_shaded.org.slf4j.LoggerFactory;

public class OwlStatusReporter {
    private static final Logger log = LoggerFactory.getLogger(OwlStatusReporter.class);
    private final String appId;
    private StatusClientWithApp statusClientWithApp;

    public OwlStatusReporter(HttpExecutor httpExecutor, String appId) {
        this.appId = appId;

        log.info("OwlStatusReporter test");
        log.error("OwlStatusReporter test");

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

    public void updateStatus(EntityRef.Type type, String id, PartialStatus partialStatus) {
        if (statusClientWithApp != null){
            try {
                log.error("Status is about to be updated.");
                statusClientWithApp.updateStatus(appId, type, id, partialStatus);
                log.error("Status updates for " + id + ".");
            } catch (Exception e) {
                log.error("An unknown exception occured during .updateStatus " + e.getMessage() + ".\n"+
                        "StatusReporter has protected you from this problem.");
            }
        } else {
            log.error("StatusReporter attempted to update a status while not being initialised properly.\n" +
                    "StatusReporter has protected you from this problem.");
        }
    }

    public void updateStatus(EntityRef.Type type, Long id, PartialStatus partialStatus) {
        log.info("update status start");
        log.error("update status start");
        if (statusClientWithApp != null){
            try {
                log.error("Status is about to be updated.");
                statusClientWithApp.updateStatus(appId, type, id, partialStatus);
                log.info("Status updates for " + id + ".");
            } catch (Exception e) {
                log.error("An unknown exception occured during .updateStatus " + e.getMessage() + ".\n"+
                        "StatusReporter has protected you from this problem.");
            }
        } else {
            log.error("StatusReporter attempted to update a status while not being initialised properly.\n" +
                    "StatusReporter has protected you from this problem.");
        }
    }
}
