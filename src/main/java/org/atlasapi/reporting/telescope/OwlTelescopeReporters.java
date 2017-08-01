package org.atlasapi.reporting.telescope;

public enum OwlTelescopeReporters implements TelescopeReporter {
    BBC_NITRO_INGEST("bbc-nitro-ingester", "BBC Nitro Scheduled Ingester"),
    BBC_NITRO_INGEST_API("bbc-nitro-ingester-api", "BBC Nitro API Ingester"),
    MOCK_REPORTER("mock-reporter", "Mock reporter")
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

