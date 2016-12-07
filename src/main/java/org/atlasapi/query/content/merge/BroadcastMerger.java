package org.atlasapi.query.content.merge;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Version;

import com.metabroadcast.common.base.Maybe;

import com.google.common.collect.Iterables;

import static com.google.gson.internal.$Gson$Preconditions.checkNotNull;

public class BroadcastMerger {

    private final VersionMerger versionMerger;

    private BroadcastMerger(
            VersionMerger versionMerger
    ){
        this.versionMerger = checkNotNull(versionMerger);
    }

    public static BroadcastMerger create(
            VersionMerger versionMerger
    ){
        return new BroadcastMerger(
                versionMerger
        );
    }

    public Content mergeBroadcasts(
            Maybe<Identified> possibleExisting,
            Content update,
            boolean merge,
            DefaultBroadcastMerger broadcastMerger
    ) {
        if (possibleExisting.isNothing()) {
            return update;
        }
        Identified existing = possibleExisting.requireValue();
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
            versionMerger.mergeVersions(existingVersion, postedVersion, merge, broadcastMerger);
        }
        return (Content) existing;
    }
}
