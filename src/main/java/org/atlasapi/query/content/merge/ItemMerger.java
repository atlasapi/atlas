package org.atlasapi.query.content.merge;

import java.util.Set;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Song;
import org.atlasapi.media.entity.Version;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import static com.google.gson.internal.$Gson$Preconditions.checkNotNull;

public class ItemMerger {
    private final VersionMerger versionMerger;
    private final SongMerger songMerger;

    private ItemMerger(
            VersionMerger versionMerger,
            SongMerger songMerger
    ){
        this.versionMerger = checkNotNull(versionMerger);
        this.songMerger = checkNotNull(songMerger);
    }

    public static ItemMerger create(
            VersionMerger versionMerger,
            SongMerger songMerger
    ){
        return new ItemMerger(
                versionMerger,
                songMerger
        );
    }

    public Item mergeItems(
            Item existing,
            Item update,
            boolean merge,
            BroadcastMerger broadcastMerger
    ) {
        if (!update.getVersions().isEmpty()) {
            if (Iterables.isEmpty(existing.getVersions())) {
                existing.addVersion(new Version());
            }
            Version existingVersion = existing.getVersions().iterator().next();
            Version postedVersion = Iterables.getOnlyElement(update.getVersions());
            versionMerger.mergeVersions(
                    existingVersion,
                    postedVersion,
                    merge,
                    broadcastMerger
            );
        }
        existing.setCountriesOfOrigin(update.getCountriesOfOrigin());
        existing.setYear(update.getYear());
        existing.setParentRef(update.getContainer());

        if (existing instanceof Song && update instanceof Song) {
            return songMerger.mergeSongs((Song) existing, (Song) update);
        }
        return existing;
    }

    public Item mergeReleaseDates(Item existing, Item update, boolean merge) {
        existing.setReleaseDates(
                merge
                ? merge(existing.getReleaseDates(), update.getReleaseDates())
                : update.getReleaseDates()
        );
        return existing;
    }

    private <T> Set<T> merge(Set<T> existing, Set<T> posted) {
        return ImmutableSet.copyOf(Iterables.concat(posted, existing));
    }
}
