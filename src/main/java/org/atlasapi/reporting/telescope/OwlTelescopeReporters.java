package org.atlasapi.reporting.telescope;

import com.metabroadcast.columbus.telescope.client.TelescopeReporterName;

/**
 * Add more elements as needed if more ingesters need to report to telescope.
 */
public enum OwlTelescopeReporters implements TelescopeReporterName {
    BBC_NITRO_INGEST("bbc-nitro-ingester", "BBC Nitro Ingester"),
    BBC_NITRO_INGEST_OFFSCHEDULE(
            "bbc-nitro-ingester-offschedule",
            "BBC Nitro off-schedule Ingester"
    ),
    BBC_NITRO_INGEST_PICKS("bbc-nitro-ingester-picks","Mbst Picks Content Group Updater"),
    BBC_NITRO_INGEST_API("bbc-nitro-ingester-api", "BBC Nitro API Ingester"),
    BBC_NITRO_INGEST_CHANNELS("bbc-nitro-ingester-channels", "BBC Nitro Channel Ingester"),
    MOCK_REPORTER("mock-reporter", "Mock reporter"), //used by OwlTelescopeProxyMock
    CHANNEL_EQUIVALENCE("channel-equivalence", "Channel Equivalence"),
    CHANNEL_SCHEDULE_EQUIVALENCE("channel-schedule-equivalence", "Channel Schedule Equivalence"),
    CHANNEL_EQUIVALENCE_UPDATE_TASK(
            "channel-equivalence-update-task",
            "Channel Equivalence Update Task")
    ,
    EQUIVALENCE("equivalence", "Equivalence"),
    EQUIVALENCE_UPDATING_WORKER("equivalence-updating-worker", "Equivalence Updating Worker"),
    CONTENT_EQUIVALENCE_UPDATE_TASK(
            "content-equivalence-update-task",
            "Content Equivalence Update Task"
    ),
    MANUAL_EQUIVALENCE("manual-equivalence", "Manual Equivalence"),
    MANUAL_CHANNEL_EQUIVALENCE("manual-channel-equivalence", "Manual Channel Equivalence"),
    QUERY_EXECUTOR_EQUIVALENCE("query-executor-equivalence", "Query Executor Equivalence")
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

