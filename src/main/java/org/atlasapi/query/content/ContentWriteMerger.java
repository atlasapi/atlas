package org.atlasapi.query.content;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import org.atlasapi.media.entity.*;
import org.joda.time.Duration;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ContentWriteMerger {

    private ContentWriteMerger() {
    }

    public static ContentWriteMerger create() {
        return new ContentWriteMerger();
    }

    public Content mergeBroadcasts(Optional<Identified> possibleExisting, Content update) {
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
            mergeVersions(existingVersion, postedVersion);
        }
        return (Content) existing;
    }

    public Content merge(Optional<Identified> possibleExisting, Content update, boolean merge) {
        if (!possibleExisting.isPresent()) {
            return update;
        }

        Identified existing = possibleExisting.get();

        if (existing instanceof Content) {
            return merge((Content) existing, update, merge);
        }
        throw new IllegalStateException("Entity for " + update.getCanonicalUri() + " not Content");
    }

    private Content merge(Content existing, Content update, boolean merge) {
        existing.setActivelyPublished(update.isActivelyPublished());

        existing.setEquivalentTo(merge ?
                merge(existing.getEquivalentTo(), update.getEquivalentTo()) :
                update.getEquivalentTo());
        existing.setLastUpdated(update.getLastUpdated());
        existing.setTitle(update.getTitle());
        existing.setShortDescription(update.getShortDescription());
        existing.setMediumDescription(update.getMediumDescription());
        existing.setLongDescription(update.getLongDescription());
        existing.setDescription(update.getDescription());
        existing.setImage(update.getImage());
        existing.setThumbnail(update.getThumbnail());
        existing.setMediaType(update.getMediaType());
        existing.setSpecialization(update.getSpecialization());
        existing.setRelatedLinks(merge ?
                merge(existing.getRelatedLinks(), update.getRelatedLinks()) :
                update.getRelatedLinks());
        existing.setAliases(merge ?
                merge(existing.getAliases(), update.getAliases()) :
                update.getAliases());
        existing.setTopicRefs(merge ?
                merge(existing.getTopicRefs(), update.getTopicRefs()) :
                update.getTopicRefs());
        existing.setPeople(merge ? merge(existing.people(), update.people()) : update.people());
        existing.setKeyPhrases(update.getKeyPhrases());
        existing.setClips(merge ?
                merge(existing.getClips(), update.getClips()) :
                update.getClips());
        existing.setPriority(update.getPriority());
        existing.setEventRefs(merge ? merge(existing.events(), update.events()) : update.events());
        existing.setImages(merge ?
                merge(existing.getImages(), update.getImages()) :
                update.getImages());
        existing.setAwards(merge ?
                merge(existing.getAwards(), update.getAwards()) :
                update.getAwards());
        existing.setPresentationChannel(update.getPresentationChannel());
        existing.setYear(update.getYear());
        existing.setLocalizedTitles(
                merge ?
                        merge(existing.getLocalizedTitles(), update.getLocalizedTitles()) :
                        update.getLocalizedTitles()
        );
        existing.setDistributions(
                merge ?
                        merge(
                                (List) existing.getDistributions(),
                                (List) update.getDistributions()
                        ) :
                update.getDistributions()
        );

        existing.setLanguage(
                merge ?
                        mergeLanguages(existing.getLanguage(), update.getLanguage()) :
                        update.getLanguage()
        );

        existing.setReviews(
                merge ?
                        merge(existing.getReviews(), update.getReviews()) :
                        update.getReviews()
        );

        if (existing instanceof Episode && update instanceof Episode) {
            mergeEpisodes((Episode) existing, (Episode) update);
        }
        if (existing instanceof Item && update instanceof Item) {
            return mergeItems(
                    mergeReleaseDates((Item) existing, (Item) update, merge),
                    (Item) update
            );
        }
        return existing;
    }

    private Language mergeLanguages(Language existing, Language update) {

        Language.Builder languageBuilder = Language.builder();

        if (update == null) {
            return existing;
        }

        if (!Strings.isNullOrEmpty(update.getCode())) {
            languageBuilder.withCode(update.getCode());
        } else {
            languageBuilder.withCode(existing.getCode());
        }
        if (!Strings.isNullOrEmpty(update.getDubbing())) {
            languageBuilder.withDubbing(update.getDubbing());
        } else {
            languageBuilder.withDubbing(existing.getCode());
        }
        if (!Strings.isNullOrEmpty(update.getDisplay())) {
            languageBuilder.withDisplay(update.getDisplay());
        } else {
            languageBuilder.withDisplay(existing.getDisplay());
        }

        return languageBuilder.build();
    }

    private Item mergeEpisodes(Episode existing, Episode update) {
        existing.setSeriesNumber(update.getSeriesNumber());
        existing.setEpisodeNumber(update.getEpisodeNumber());
        return existing;
    }

    private Item mergeReleaseDates(Item existing, Item update, boolean merge) {
        existing.setReleaseDates((merge ?
                merge(existing.getReleaseDates(), update.getReleaseDates()) :
                update.getReleaseDates()
        ));
        return existing;
    }

    private Item mergeItems(Item existing, Item update) {
        if (!update.getVersions().isEmpty()) {
            if (Iterables.isEmpty(existing.getVersions())) {
                existing.addVersion(new Version());
            }
            Version existingVersion = existing.getVersions().iterator().next();
            Version postedVersion = Iterables.getOnlyElement(update.getVersions());
            mergeVersions(existingVersion, postedVersion);
        }
        existing.setCountriesOfOrigin(update.getCountriesOfOrigin());
        existing.setYear(update.getYear());
        if (existing instanceof Song && update instanceof Song) {
            return mergeSongs((Song) existing, (Song) update);
        }
        return existing;
    }

    private void mergeVersions(Version existing, Version update) {
        Integer updatedDuration = update.getDuration();
        if (updatedDuration != null) {
            existing.setDuration(Duration.standardSeconds(updatedDuration));
        } else {
            existing.setDuration(null);
        }
        existing.setManifestedAs(update.getManifestedAs());
        existing.setBroadcasts(update.getBroadcasts());
        existing.setSegmentEvents(update.getSegmentEvents());
        existing.setRestriction(update.getRestriction());
    }

    private Song mergeSongs(Song existing, Song update) {
        existing.setIsrc(update.getIsrc());
        existing.setDuration(update.getDuration());
        return existing;
    }

    private <T> Set<T> merge(Set<T> existing, Set<T> posted) {
        return ImmutableSet.copyOf(Iterables.concat(posted, existing));
    }

    private <T> List<T> merge(List<T> existing, List<T> posted) {
        return ImmutableSet.copyOf(Iterables.concat(posted, existing)).asList();
    }
}