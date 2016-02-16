package org.atlasapi.remotesite;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Version;
import org.joda.time.Duration;

import com.google.common.base.Equivalence;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ContentMerger {

    private static interface TopicMergeStrategy {
        Content mergeTopics(Content current, Content extracted);
    }

    private static interface VersionMergeStrategy {
        Content mergeVersions(Content current, Content extracted);
    }
    
    static interface AliasMergeStrategy {
        Item mergeAliases(Item current, Item extracted);
    }

    public static abstract class MergeStrategy {
        private MergeStrategy(){}

        public static final LeaveEverythingAlone KEEP = new LeaveEverythingAlone();
        public static final ReplaceEverything REPLACE = new ReplaceEverything();
        public static final StandardMerge MERGE = new StandardMerge();

        public static ReplaceTopicsBasedOnEquivalence replaceTopicsBasedOn(final Equivalence<TopicRef> equivalence) {
            Preconditions.checkNotNull(equivalence);
            return new ReplaceTopicsBasedOnEquivalence(equivalence);
        }
    }

    private final VersionMergeStrategy versionMergeStrategy;
    private final TopicMergeStrategy topicsMergeStrategy;
    private final AliasMergeStrategy aliasMergeStrategy;

    public ContentMerger(VersionMergeStrategy versionMergeStrategy, TopicMergeStrategy topicsMergeStrategy, 
            AliasMergeStrategy aliasMergeStrategy) {
        this.versionMergeStrategy = checkNotNull(versionMergeStrategy);
        this.topicsMergeStrategy = checkNotNull(topicsMergeStrategy);
        this.aliasMergeStrategy = checkNotNull(aliasMergeStrategy);
    }

    public Item merge(Item current, Item extracted) {

        current = aliasMergeStrategy.mergeAliases(current, extracted);
        current = mergeContents(current, extracted);

        if ( current.getContainer() == null
                || extracted.getContainer() == null
                || !current.getContainer().getUri().equals(extracted.getContainer().getUri())) {
            current.setParentRef(extracted.getContainer());
        }
        
        if (current instanceof Episode) {
            if (extracted instanceof Episode) {
                current = mergeEpisodeSpecificFields((Episode) current, (Episode) extracted);
            } else if (extracted instanceof Item) {
                Item newItem = new Item();
                Item.copyTo(current, newItem);
                current = newItem;
            }
        } else if (current instanceof Item) {
            if (extracted instanceof Episode) {
                Episode newEp = new Episode();
                Item.copyTo(current, newEp);
                
                current = mergeEpisodeSpecificFields(newEp, (Episode) extracted);
            }
        }

        if (current instanceof Film && extracted instanceof Film) {
            Film currentFilm = (Film) current;
            Film extractedFilm = (Film) extracted;
            currentFilm.setYear(extractedFilm.getYear());
        } else (if current instanceof Item && extracted instanceof Film) {

            // The type is switching from Item to Film; we must use the extracted
            // Film as a basis of saving, but retain those fields that have been
            // merged already onto current, according to the employed merge strategy
            extracted.setVersions(current.getVersions());
            extracted.setAliases(current.getAliases());
            extracted.setTopicRefs(current.getTopicRefs());
            current = extracted;
        }
        current.setReleaseDates(extracted.getReleaseDates());
        return current;
    }

    private Episode mergeEpisodeSpecificFields(Episode current, Episode extracted) {
        current.setEpisodeNumber(extracted.getEpisodeNumber());
        current.setSeriesNumber(extracted.getSeriesNumber());

        if ( current.getSeriesRef() == null
                || extracted.getSeriesRef() == null
                || !current.getSeriesRef().getUri().equals(extracted.getSeriesRef().getUri())) {
            current.setSeriesRef(extracted.getSeriesRef());
        }
        return current;
    }

    public Container merge(Container current, Container extracted) {
        current = mergeContents(current, extracted);
        if (current instanceof Series && extracted instanceof Series) {
            Series currentSeries = (Series) current;
            Series extractedSeries = (Series) extracted;
            
            currentSeries.withSeriesNumber(extractedSeries.getSeriesNumber());
            
            if (currentSeries.getParent() == null
                    || extractedSeries.getParent() == null
                    || !currentSeries.getParent().equals(extractedSeries.getParent())) {
                currentSeries.setParentRef(extractedSeries.getParent());
            }
            
        }
        return current;
    }

    private <C extends Content> C mergeContents(C current, C extracted) {
        current.setActivelyPublished(extracted.isActivelyPublished());
        current.setTitle(extracted.getTitle());
        current.setDescription(extracted.getDescription());
        current.setShortDescription(extracted.getShortDescription());
        current.setMediumDescription(extracted.getMediumDescription());
        current.setLongDescription(current.getLongDescription());
        current.setImage(extracted.getImage());
        if (extracted.getImages() != null) {
            current.setImages(extracted.getImages());
        }
        current.setYear(extracted.getYear());
        current.setGenres(extracted.getGenres());
        current.setPeople(extracted.people());
        current.setLanguages(extracted.getLanguages());
        current.setCertificates(extracted.getCertificates());
        current.setMediaType(extracted.getMediaType());
        current.setSpecialization(extracted.getSpecialization());
        current.setLastUpdated(extracted.getLastUpdated());
        current.setClips(extracted.getClips());
        current.setEquivalentTo(extracted.getEquivalentTo());
        current.setRelatedLinks(extracted.getRelatedLinks());
        current.setPresentationChannel(extracted.getPresentationChannel());
        current.setMediaType(extracted.getMediaType());
        current.setSpecialization(extracted.getSpecialization());
        current.setPriority(extracted.getPriority());
        topicsMergeStrategy.mergeTopics(current, extracted);
        versionMergeStrategy.mergeVersions(current, extracted);

        return current;
    }

    public static Container asContainer(Identified identified) {
        return castTo(identified, Container.class);
    }

    public static Item asItem(Identified identified) {
        return castTo(identified, Item.class);
    }

    private static <T> T castTo(Identified identified, Class<T> cls) {
        try {
            return cls.cast(identified);
        } catch (ClassCastException e) {
            throw new ClassCastException(String.format("%s: expected %s got %s",
                    identified.getCanonicalUri(),
                    cls.getSimpleName(),
                    identified.getClass().getSimpleName()));
        }
    }

    private static class LeaveEverythingAlone extends MergeStrategy implements TopicMergeStrategy, VersionMergeStrategy, AliasMergeStrategy {
        @Override
        public Content mergeTopics(Content current, Content extracted) {
            return current;
        }

        @Override
        public Content mergeVersions(Content current, Content extracted) {
            return current;
        }

        @Override
        public Item mergeAliases(Item current, Item extracted) {
            return current;
        }
        
    }

    private static class ReplaceEverything extends MergeStrategy implements TopicMergeStrategy, VersionMergeStrategy, AliasMergeStrategy {
        @Override
        public Content mergeTopics(Content current, Content extracted) {
            current.setTopicRefs(extracted.getTopicRefs());
            return current;
        }

        @Override
        public Content mergeVersions(Content current, Content extracted) {
            current.setVersions(extracted.getVersions());
            return current;
        }

        @Override
        public Item mergeAliases(Item current, Item extracted) {
            current.setAliases(extracted.getAliases());
            current.setAliasUrls(extracted.getAliasUrls());
            return current;
        }
    }

    private static class StandardMerge extends MergeStrategy implements VersionMergeStrategy, AliasMergeStrategy, TopicMergeStrategy {
        @Override
        public Content mergeVersions(Content current, Content extracted) {
            Map<String, Version> mergedVersions = Maps.newHashMap();
            for (Version version : current.getVersions()) {
                mergedVersions.put(version.getCanonicalUri(), version);
            }
            for (Version version : extracted.getVersions()) {
                if (mergedVersions.containsKey(version.getCanonicalUri())) {
                    Version mergedVersion = mergedVersions.get(version.getCanonicalUri());
                    mergedVersion.setBroadcasts(Sets.union(version.getBroadcasts(), mergedVersion.getBroadcasts()));
                    mergedVersion.setManifestedAs(version.getManifestedAs());
                    mergedVersion.setRestriction(version.getRestriction());
                    if (version.getDuration() == null) {
                        mergedVersion.setDuration(null);
                    } else {
                        mergedVersion.setDuration(Duration.standardSeconds(version.getDuration()));
                    }
                    mergedVersions.put(version.getCanonicalUri(), mergedVersion);
                } else {
                    mergedVersions.put(version.getCanonicalUri(), version);
                }
            }
            current.setVersions(Sets.newHashSet(mergedVersions.values()));
            return current;
        }

        @Override
        public Item mergeAliases(Item current, Item extracted) {
            current.setAliases(Sets.union(current.getAliases(), extracted.getAliases()));
            current.setAliasUrls(Sets.union(current.getAliasUrls(), extracted.getAliasUrls()));
            return current;
        }

        @Override
        public Content mergeTopics(Content current, Content extracted) {
            current.setTopicRefs(
                    Sets.union(
                            ImmutableSet.copyOf(current.getTopicRefs()),
                            ImmutableSet.copyOf(extracted.getTopicRefs())
                    )
            );
            return current;
        }
    }

    private static class ReplaceTopicsBasedOnEquivalence extends MergeStrategy implements TopicMergeStrategy {
        private final Equivalence<TopicRef> equivalence;

        private ReplaceTopicsBasedOnEquivalence(Equivalence<TopicRef> equivalence) {
            this.equivalence = equivalence;
        }

        @Override
        public Content mergeTopics(Content current, Content extracted) {
            Set<Equivalence.Wrapper<TopicRef>> mergedRefs = new HashSet<>();

            for (TopicRef topicRef : current.getTopicRefs()) {
                mergedRefs.add(equivalence.wrap(topicRef));
            }
            for (TopicRef topicRef : extracted.getTopicRefs()) {
                Equivalence.Wrapper<TopicRef> wrapped = equivalence.wrap(topicRef);
                if (! mergedRefs.add(wrapped)) {
                    mergedRefs.remove(wrapped);  // force replacement
                    mergedRefs.add(wrapped);
                }
            }

            current.setTopicRefs(Iterables.transform(mergedRefs, new Function<Equivalence.Wrapper<TopicRef>, TopicRef>() {
                @Override
                public TopicRef apply(Equivalence.Wrapper<TopicRef> input) {
                    return input.get();
                }
            }));

            return current;
        }
    }

}
