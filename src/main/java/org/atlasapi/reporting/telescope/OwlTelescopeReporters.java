package org.atlasapi.reporting.telescope;

import com.metabroadcast.columbus.telescope.client.TelescopeReporterName;

/**
 * Add more elements as needed if more ingesters need to report to telescope.
 */
public enum OwlTelescopeReporters implements TelescopeReporterName {
    BBC_NITRO_INGEST_TODAY("bbc-nitro-ingester-today", "Nitro today"),
    BBC_NITRO_INGEST_TODAY_FULL_FETCH("bbc-nitro-ingester-today-ff", "Nitro today full fetch"),
    BBC_NITRO_INGEST_M7_7_DAY("bbc-nitro-ingester-m7-7", "Nitro -7 to +7 day"),
    BBC_NITRO_INGEST_M8_M30_FULL_FETCH("bbc-nitro-ingester-m8-m30-ff", "Nitro -8 to -30 full fetch "),
    BBC_NITRO_INGEST_M7_3_FULL_FETCH("bbc-nitro-ingester-m7-3-ff", "Nitro -7 to +3 day full fetch"),
    BBC_NITRO_INGEST_OFFSCHEDULE("bbc-nitro-ingester-offschedule", "Nitro off-schedule Ingester"),
    BBC_NITRO_INGEST_API("bbc-nitro-ingester-api", "Nitro API Ingester"),
    BBC_NITRO_INGEST_CHANNELS("bbc-nitro-ingester-channels", "Nitro Channel Ingester"),

    PICKS_CONTENT_GROUP_UPDATER("picks-content-group-updater","Mbst Picks Content Group Updater"),
    
    CHANNEL_EQUIVALENCE("channel-equivalence", "Channel Equivalence"),
    CHANNEL_SCHEDULE_EQUIVALENCE("channel-schedule-equivalence", "Channel Schedule Equivalence"),
    CHANNEL_EQUIVALENCE_UPDATE_TASK("channel-equivalence-update-task", "Channel Equivalence Update Task"),
    EQUIVALENCE("equivalence", "Equivalence"),
    EQUIVALENCE_UPDATING_WORKER("equivalence-updating-worker", "Equivalence Updating Worker"),
    CONTENT_EQUIVALENCE_UPDATE_TASK("content-equivalence-update-task", "Content Equivalence Update Task"),
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

