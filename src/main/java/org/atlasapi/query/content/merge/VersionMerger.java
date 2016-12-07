package org.atlasapi.query.content.merge;

import org.atlasapi.media.entity.Version;

import org.joda.time.Duration;

public class VersionMerger {

    private VersionMerger(){
    }

    public static VersionMerger create(){
        return new VersionMerger();
    }

    public void mergeVersions(
            Version existing,
            Version update,
            boolean merge,
            DefaultBroadcastMerger defaultBroadcastMerger
    ) {
        Integer updatedDuration = update.getDuration();
        if (updatedDuration != null) {
            existing.setDuration(Duration.standardSeconds(updatedDuration));
        } else {
            existing.setDuration(null);
        }
        existing.setManifestedAs(update.getManifestedAs());

        existing.setBroadcasts(defaultBroadcastMerger.merge(
                update.getBroadcasts(),
                existing.getBroadcasts(),
                merge
        ));

        existing.setSegmentEvents(update.getSegmentEvents());
        existing.setRestriction(update.getRestriction());
    }
}
