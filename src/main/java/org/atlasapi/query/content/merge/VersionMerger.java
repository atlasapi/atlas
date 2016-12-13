package org.atlasapi.query.content.merge;

import java.util.Optional;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Version;

import com.metabroadcast.common.base.Maybe;

import com.google.common.collect.Iterables;
import org.joda.time.Duration;

public class VersionMerger {

    private VersionMerger(){
    }

    public static VersionMerger create(){
        return new VersionMerger();
    }

    public Content mergeBroadcasts(
            Optional<Identified> possibleExisting,
            Content update,
            boolean merge,
            BroadcastMerger broadcastMerger
    ) {
        if (!possibleExisting.isPresent()) {
            return update;
        }
        Identified existing = possibleExisting.get();
        if (!(existing instanceof Item)) {
            throw new IllegalStateException("Entity for "
                    + update.getCanonicalUri()
                    + " not Content");
        }
        Item item = (Item) existing;
        if (!update.getVersions().isEmpty()) {
            if (Iterables.isEmpty(item.getVersions())) {
                item.addVersion(new Version());
            }
            Version existingVersion = item.getVersions().iterator().next();
            Version postedVersion = Iterables.getOnlyElement(update.getVersions());
            mergeVersions(existingVersion, postedVersion, merge, broadcastMerger);
        }
        return (Content) existing;
    }

    public void mergeVersions(
            Version existing,
            Version update,
            boolean merge,
            BroadcastMerger broadcastMerger
    ) {
        Integer updatedDuration = update.getDuration();
        if (updatedDuration != null) {
            existing.setDuration(Duration.standardSeconds(updatedDuration));
        } else {
            existing.setDuration(null);
        }
        existing.setManifestedAs(update.getManifestedAs());

        existing.setBroadcasts(broadcastMerger.merge(
                update.getBroadcasts(),
                existing.getBroadcasts(),
                merge
        ));

        existing.setSegmentEvents(update.getSegmentEvents());
        existing.setRestriction(update.getRestriction());
    }
}
