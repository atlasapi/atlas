package org.atlasapi.reporting.telescope;

import com.metabroadcast.columbus.telescope.client.TelescopeReporterName;

/**
 * Add more elements as needed if more ingesters need to report to telescope.
 */
public enum OwlTelescopeReporters implements TelescopeReporterName {
    BBC_NITRO_INGEST("bbc-nitro-ingester", "BBC Nitro Ingester"),
    BBC_NITRO_INGEST_OFFSCHEDULE("bbc-nitro-ingester-offschedule", "BBC Nitro off-schedule Ingester"),
    BBC_NITRO_INGEST_PICKS("bbc-nitro-ingester-picks","Mbst Picks Content Group Updater"),
    BBC_NITRO_INGEST_API("bbc-nitro-ingester-api", "BBC Nitro API Ingester"),
    BBC_NITRO_INGEST_CHANNELS("bbc-nitro-ingester-channels", "BBC Nitro Channel Ingester"),
    ;

    final String reporterKey;
    final String reporterName;

    OwlTelescopeReporters(String reporterKey, String reporterName) {
        this.reporterKey = reporterKey;
        this.reporterName = reporterName;
    }

    public String getReporterKey() {
        return reporterKey;
    }

    public String getReporterName() {
        return reporterName;
    }
}

