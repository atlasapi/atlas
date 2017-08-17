package org.atlasapi.reporting.telescope;

import com.metabroadcast.columbus.telescope.client.TelescopeReporterName;

/**
 * Add more elements as needed if more ingesters need to report to telescope.
 */
public enum OwlTelescopeReporters implements TelescopeReporterName {
    BBC_NITRO_INGEST("bbc-nitro-scheduled-ingester", "BBC Nitro Scheduled Ingester"),
    BBC_NITRO_INGEST_API("bbc-nitro-ingester-api", "BBC Nitro API Ingester"),
    MOCK_REPORTER("mock-reporter", "Mock reporter"), //used by OwlTelescopeProxyMock
    CHANNEL_EQUIVALENCE("channel-equivalence", "Channel Equivalence"),
    EQUIVALENCE("equivalence", "Equivalence"),
    MANUAL_EQUIVALENCE("manual-equivalence", "Manual Equivalence"),
    PUBLISHER_SPECIFIC_EQUIVALENCE("publisher-specific-equivalence", "Publisher Specific Equivalence")
    ;

    String reporterKey;
    String reporterName;

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

