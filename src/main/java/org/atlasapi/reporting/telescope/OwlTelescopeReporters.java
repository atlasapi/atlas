package org.atlasapi.reporting.telescope;

import com.metabroadcast.columbus.telescope.client.TelescopeReporterName;

/**
 * Add more elements as needed if more ingesters need to report to telescope.
 */
public enum OwlTelescopeReporters implements TelescopeReporterName {
    // naming the key with dot namespacing allows for grouping permissions when it comes to
    // viewing events, so be mindful of your names.
    // Other names might exist in different projects so make some effort to avoid collisions.
    BBC_NITRO_INGEST_TODAY("bbc.nitro.ingester.today", "Nitro today"),
    BBC_NITRO_INGEST_TODAY_FULL_FETCH("bbc.nitro.ingester.today-ff", "Nitro today full fetch"),
    BBC_NITRO_INGEST_M7_7_DAY("bbc.nitro.ingester.m7-7", "Nitro -7 to +7 day"),
    BBC_NITRO_INGEST_M8_M30_FULL_FETCH("bbc.nitro.ingester.m8-m30-ff", "Nitro -8 to -30 full fetch "),
    BBC_NITRO_INGEST_M7_3_FULL_FETCH("bbc.nitro.ingester.m7-3-ff", "Nitro -7 to +3 day full fetch"),
    BBC_NITRO_INGEST_OFFSCHEDULE("bbc.nitro.ingester.offschedule", "Nitro off-schedule Ingester"),
    BBC_NITRO_INGEST_API("bbc.nitro.ingester.api", "Nitro API Ingester"),
    BBC_NITRO_INGEST_CHANNELS("bbc.nitro.ingester.channels", "Nitro Channel Ingester"),
    
    BARB_INGEST_CHANNELS("barb.ingester.channels", "Barb Channel Ingester"),

    CHANNEL_4_INGEST("channel4.ingester", "Channel 4 Ingester"),

    AMAZON_PRIME_VIDEO_UPDATE_TASK("amazon.ingester","Amazon Ingester"),

    RADIO_TIMES_INGESTER("radiotimes.ingester", "RadioTimes Film Ingester"),

    PICKS_CONTENT_GROUP_UPDATER("mbst-picks-content-group-updater","Mbst Picks Content Group Updater"),
    
    CHANNEL_EQUIVALENCE("equivalence.channel.auto", "Channel Equivalence"),
    CHANNEL_SCHEDULE_EQUIVALENCE("equivalence.channel-schedule", "Channel Schedule Equivalence"),
    CHANNEL_EQUIVALENCE_UPDATE_TASK("equivalence.channel-update-task", "Channel Equivalence Update Task"),
    EQUIVALENCE("equivalence.generic", "Equivalence"),
    EQUIVALENCE_UPDATING_WORKER("equivalence.updating-worker", "Equivalence Updating Worker"),
    CONTENT_EQUIVALENCE_UPDATE_TASK("equivalence.content-update-task", "Content Equivalence Update Task"),
    MANUAL_EQUIVALENCE("equivalence.manual", "Manual Equivalence"),
    MANUAL_CHANNEL_EQUIVALENCE("equivalence.channel.manual", "Manual Channel Equivalence"),
    QUERY_EXECUTOR_EQUIVALENCE("equivalence.query-executor", "Query Executor Equivalence")
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

