package org.atlasapi.equiv.handlers;

import static org.atlasapi.media.entity.ChildRef.TO_URI;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.equiv.EquivalenceSummary;
import org.atlasapi.equiv.EquivalenceSummaryStore;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.LookupWriter;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableMultimap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.metabroadcast.common.collect.OptionalMap;

public class EpisodeMatchingEquivalenceHandler implements EquivalenceResultHandler<Container> {

    private final static Function<ScoredCandidate<Container>, Container> TO_CONTAINER = ScoredCandidate.<Container>toCandidate();
    
    private final EquivalenceSummaryStore summaryStore;
    private final ContentResolver contentResolver;
    private final LookupWriter lookupWriter;
    private final Set<Publisher> publishers;


    public EpisodeMatchingEquivalenceHandler(ContentResolver contentResolver, EquivalenceSummaryStore summaryStore, LookupWriter lookupWriter, Iterable<Publisher> acceptablePublishers) {
        this.contentResolver = contentResolver;
        this.summaryStore = summaryStore;
        this.lookupWriter = lookupWriter;
        this.publishers = ImmutableSet.copyOf(acceptablePublishers);
    }
    
    @Override
    public void handle(EquivalenceResult<Container> result) {
        result.description().startStage("Episode sequence stitching");
        
        Collection<Container> equivalentContainers = Collections2.transform(result.strongEquivalences().values(), TO_CONTAINER);
        Iterable<Episode> subjectsChildren = childrenOf(result.subject());
        Multimap<Container, Episode> equivalentsChildren = childrenOf(equivalentContainers);
        OptionalMap<String,EquivalenceSummary> childSummaries = summaryStore.summariesForUris(Iterables.transform(result.subject().getChildRefs(), ChildRef.TO_URI));
        Map<String, EquivalenceSummary> summaryMap = summaryMap(childSummaries);
        
        stitch(subjectsChildren, summaryMap, equivalentsChildren, result.description());
        
        result.description().finishStage();
    }

    private void stitch(Iterable<Episode> subjectsChildren, Map<String, EquivalenceSummary> summaryMap, Multimap<Container, Episode> equivalentsChildren, ReadableDescription desc) {
        for (Episode episode : subjectsChildren) {
            EquivalenceSummary summary = summaryMap.get(episode.getCanonicalUri());
            if (summary != null) {
                stitch(episode, summary, equivalentsChildren, desc);
            }
        }
    }

    private void stitch(Episode subjectEpisode, EquivalenceSummary equivalenceSummary, Multimap<Container, Episode> equivalentsChildren, ReadableDescription desc) {
        String subjectUri = subjectEpisode.getCanonicalUri();
        desc.startStage(subjectUri);
        Multimap<Publisher, ContentRef> equivalents = equivalenceSummary.getEquivalents();

        Set<ContentRef> additionalEquivs = Sets.newHashSet();
        for (Entry<Container, Collection<Episode>> contentHierarchy : equivalentsChildren.asMap().entrySet()) {
            Container container = contentHierarchy.getKey();
            for (Episode equivChild : contentHierarchy.getValue()) {
                if(matchingSequenceNumbers(subjectEpisode, equivChild)) {
                    Publisher publisher = equivChild.getPublisher();
                    Collection<ContentRef> existingEquiv = equivalents.get(publisher);
                    if (!existingEquiv.isEmpty()) {
                        desc.appendText("existing strong equiv %s not overwritten by %s",existingEquiv, equivChild);
                    } else {
                        desc.appendText("adding %s (%s)", equivChild, container);
                        additionalEquivs.add(ContentRef.valueOf(equivChild));
                    }
                    break;
                }
            }
        }
        
        if (!additionalEquivs.isEmpty()) {
            additionalEquivs.addAll(equivalents.values());
            lookupWriter.writeLookup(ContentRef.valueOf(subjectEpisode), additionalEquivs, publishers);
        }

        desc.finishStage();
    }
    
    public boolean matchingSequenceNumbers(Episode target, Episode ep) {
        return target.getEpisodeNumber() != null 
            && target.getEpisodeNumber().equals(ep.getEpisodeNumber())
            && target.getSeriesNumber() != null
            && target.getSeriesNumber().equals(ep.getSeriesNumber());
    }

    private Map<String, EquivalenceSummary> summaryMap(OptionalMap<String, EquivalenceSummary> childSummaries) {
        return Maps.filterValues(Maps.transformValues(childSummaries, new Function<Optional<EquivalenceSummary>, EquivalenceSummary>() {
            @Override
            public EquivalenceSummary apply(@Nullable Optional<EquivalenceSummary> input) {
                return input.orNull();
            }
        }), Predicates.notNull());
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
