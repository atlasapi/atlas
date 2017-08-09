package org.atlasapi.reporting.telescope;

/**
 * Add more elements as needed if more ingesters need to report to telescope.
 */
public enum OwlTelescopeReporters implements TelescopeReporter {
    BBC_NITRO_INGEST("bbc-nitro-scheduled-ingester", "BBC Nitro Scheduled Ingester"),
    BBC_NITRO_INGEST_API("bbc-nitro-ingester-api", "BBC Nitro API Ingester"),
    MOCK_REPORTER("mock-reporter", "Mock reporter"), //used by OwlTelescopeProxyMock
    EQUIVALENCE("equivalence", "Equivalence")
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

