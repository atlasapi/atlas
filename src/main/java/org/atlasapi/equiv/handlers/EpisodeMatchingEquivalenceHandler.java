package org.atlasapi.equiv.handlers;

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableMultimap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.metabroadcast.common.collect.OptionalMap;
import org.atlasapi.equiv.ContentRef;
import org.atlasapi.equiv.EquivalenceSummary;
import org.atlasapi.equiv.EquivalenceSummaryStore;
import org.atlasapi.equiv.results.EquivalenceResults;
import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.atlasapi.media.entity.ChildRef.TO_URI;

/**
 * This handler will resolve all the children of the subject container and its strong equivalences
 * If any child A of the subject container matches on both series number and episode number to a child B
 * of the strong equivalences (and it is a publisher for which child A has no strong equivalences)
 * then child A will have its equivalences updated to include child B.
 * N.B. this has the consequence that directly running equiv on child A will remove the link to child B until
 * equivalence is rerun on child A's container. This came as quite a surprise to some of us years down the line since
 * this hadn't been documented!
 */
public class EpisodeMatchingEquivalenceHandler implements EquivalenceResultHandler<Container> {
    private final Logger log = LoggerFactory.getLogger(EpisodeMatchingEquivalenceHandler.class);
    
    private final EquivalenceSummaryStore summaryStore;
    private final ContentResolver contentResolver;
    private final LookupWriter lookupWriter;
    private final Set<Publisher> publishers;

    public EpisodeMatchingEquivalenceHandler(
            ContentResolver contentResolver,
            EquivalenceSummaryStore summaryStore,
            LookupWriter lookupWriter,
            Iterable<Publisher> acceptablePublishers
    ) {
        this.contentResolver = contentResolver;
        this.summaryStore = summaryStore;
        this.lookupWriter = lookupWriter;
        this.publishers = ImmutableSet.copyOf(acceptablePublishers);
    }
    
    @Override
    public boolean handle(EquivalenceResults<Container> results) {
        results.description().startStage("Episode equivalence updater using series & episode numbers");
        
        Set<Container> equivalentContainers = results.strongEquivalences();

        Iterable<Episode> subjectsChildren = childrenOf(results.subject());
        Multimap<Container, Episode> equivalentsChildren = childrenOf(equivalentContainers);
        OptionalMap<String,EquivalenceSummary> childSummaries = summaryStore.summariesForUris(
                Iterables.transform(results.subject().getChildRefs(), ChildRef.TO_URI)
        );
        Map<String, EquivalenceSummary> summaryMap = summaryMap(childSummaries);
        
        boolean handledWithStateChange = stitch(
                subjectsChildren,
                summaryMap,
                equivalentsChildren,
                results.description()
        );
        
        results.description().finishStage();

        return handledWithStateChange;
    }

    private boolean stitch(
            Iterable<Episode> subjectsChildren,
            Map<String, EquivalenceSummary> summaryMap,
            Multimap<Container, Episode> equivalentsChildren,
            ReadableDescription desc
    ) {
        boolean handledWithStateChange = false;

        for (Episode episode : subjectsChildren) {
            EquivalenceSummary summary = summaryMap.get(episode.getCanonicalUri());
            if (summary != null) {
                handledWithStateChange |= stitch(episode, summary, equivalentsChildren, desc);
            }
        }

        return handledWithStateChange;
    }

    private boolean stitch(
            Episode subjectEpisode,
            EquivalenceSummary equivalenceSummary,
            Multimap<Container, Episode> equivalentsChildren,
            ReadableDescription desc
    ) {
        String subjectUri = subjectEpisode.getCanonicalUri();
        desc.startStage("Checking child: " + subjectUri);
        Multimap<Publisher, ContentRef> equivalents = equivalenceSummary.getEquivalents();

        Set<ContentRef> additionalEquivs = Sets.newHashSet();
        for (Entry<Container, Collection<Episode>> contentHierarchy
                : equivalentsChildren.asMap().entrySet()) {
            Container container = contentHierarchy.getKey();
            for (Episode equivChild : contentHierarchy.getValue()) {
                if(matchingSequenceNumbers(subjectEpisode, equivChild)) {
                    Publisher publisher = equivChild.getPublisher();
                    Collection<ContentRef> existingEquiv = equivalents.get(publisher);
                    if (!existingEquiv.isEmpty()) {
                        desc.appendText(
                                "existing strong equiv %s not overwritten by %s",
                                existingEquiv,
                                equivChild
                        );
                    } else {
                        log.info("Adding {} as an equiv for child {}", equivChild, subjectEpisode);
                        desc.appendText("adding %s (%s) as an equiv for this child", equivChild, container);
                        additionalEquivs.add(ContentRef.valueOf(equivChild));
                    }
                    break;
                }
            }
        }

        boolean handledWithStateChange = false;

        if (!additionalEquivs.isEmpty()) {
            additionalEquivs.addAll(equivalents.values());
            handledWithStateChange = lookupWriter.writeLookup(
                    ContentRef.valueOf(subjectEpisode),
                    additionalEquivs,
                    publishers
            )
                    .isPresent();
        }

        desc.finishStage();

        return handledWithStateChange;
    }
    
    private boolean matchingSequenceNumbers(Episode target, Episode ep) {
        return target.getEpisodeNumber() != null 
            && target.getEpisodeNumber().equals(ep.getEpisodeNumber())
            && target.getSeriesNumber() != null
            && target.getSeriesNumber().equals(ep.getSeriesNumber());
    }

    private Map<String, EquivalenceSummary> summaryMap(
            OptionalMap<String, EquivalenceSummary> childSummaries
    ) {
        return Maps.filterValues(
                Maps.transformValues(childSummaries, Optional::orNull),
                Predicates.notNull()
        );
    }

    private Multimap<Container, Episode> childrenOf(Iterable<Container> equivalentContainers) {
        Builder<Container, Episode> builder = ImmutableMultimap.builder();
        for (Container container : equivalentContainers) {
            builder.putAll(container, childrenOf(container));
        }
        return builder.build();
    }

    private Iterable<Episode> childrenOf(Container container) {
        ImmutableList<ChildRef> childRefs = container.getChildRefs();
        Iterable<String> childUris = Iterables.transform(childRefs, TO_URI);
        ResolvedContent children = contentResolver.findByCanonicalUris(childUris);
        return Iterables.filter(children.getAllResolvedResults(), Episode.class);
    }
}
