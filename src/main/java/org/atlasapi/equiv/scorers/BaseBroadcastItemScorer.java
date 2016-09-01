package org.atlasapi.equiv.scorers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nullable;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import com.metabroadcast.common.base.Maybe;

/**
 * <p>Specialized {@link EquivalenceScorer} for "broadcast items", i.e. those that
 * have exactly one {@link org.atlasapi.media.entity.Broadcast Broadcast} per
 * {@link Item} rather than many.</p>
 * 
 * For each candidate, it will check for matches, in order:
 * <ol>
 * <li>The subject against the candidate</li>
 * <li>The subject's {@link Container} against the candidate</li>
 * <li>The candidate's Container against the subject.
 * </ol>
 * 
 * <p>Subclasses must implement each of the above 3 checks.</p> 
 * 
 * <p>Any match results in a score of {@link Score#ONE one} otherwise the score is
 * configured mismatch score.</p>
 */
public abstract class BaseBroadcastItemScorer implements EquivalenceScorer<Item> {
    
    private final ContentResolver resolver;
    private final Score misMatchScore;
    private final Set<String> SPORTS_CHANNEL = new TreeSet<>(ImmutableSet.of(
            "http://ref.atlasapi.org/channels/pressassociation.com/1745",
            "http://ref.atlasapi.org/channels/pressassociation.com/1970",
            "http://ref.atlasapi.org/channels/theactivechannel",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/1045",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/150",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/195",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/955",
            "http://ref.atlasapi.org/channels/liverpoolfctv",
            "http://ref.atlasapi.org/channels/pressassociation.com/1679",
            "http://ref.atlasapi.org/channels/pressassociation.com/1678",
            "http://ref.atlasapi.org/channels/pressassociation.com/1677",
            "http://ref.atlasapi.org/channels/pressassociation.com/1676",
            "http://ref.atlasapi.org/channels/pressassociation.com/1899",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/1150",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/1158",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/1155",
            "http://ref.atlasapi.org/channels/pressassociation.com/1957",
            "http://ref.atlasapi.org/channels/pressassociation.com/1958",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/944",
            "http://ref.atlasapi.org/channels/setantaireland",
            "http://ref.atlasapi.org/channels/pressassociation.com/1745",
            "http://ref.atlasapi.org/channels/pressassociation.com/1970",
            "http://ref.atlasapi.org/channels/theactivechannel",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/1045",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/150",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/195",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/955",
            "http://ref.atlasapi.org/channels/liverpoolfctv",
            "http://ref.atlasapi.org/channels/pressassociation.com/1679",
            "http://ref.atlasapi.org/channels/pressassociation.com/1678",
            "http://ref.atlasapi.org/channels/pressassociation.com/1677",
            "http://ref.atlasapi.org/channels/pressassociation.com/1676",
            "http://ref.atlasapi.org/channels/pressassociation.com/1899",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/1150",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/1158",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/1155",
            "http://ref.atlasapi.org/channels/pressassociation.com/1957",
            "http://ref.atlasapi.org/channels/pressassociation.com/1958",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/944",
            "http://ref.atlasapi.org/channels/setantaireland",
            "http://ref.atlasapi.org/channels/pressassociation.com/1896",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/1046",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/1110",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/152",
            "http://ref.atlasapi.org/channels/pressassociation.com/1928",
            "http://ref.atlasapi.org/channels/racinguk",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/652",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/1139",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/637",
            "http://ref.atlasapi.org/channels/pressassociation.com/1941",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/877",
            "http://ref.atlasapi.org/channels/espn",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/1156",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/1157",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/1152",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/1154",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/1153",
            "http://ref.atlasapi.org/channels/pressassociation.com/1951",
            "http://ref.atlasapi.org/channels/pressassociation.com/1954",
            "http://ref.atlasapi.org/channels/pressassociation.com/1962",
            "http://ref.atlasapi.org/channels/pressassociation.com/1961",
            "http://ref.atlasapi.org/channels/pressassociation.com/1966",
            "http://ref.atlasapi.org/channels/pressassociation.com/1965",
            "http://ref.atlasapi.org/channels/pressassociation.com/1955",
            "http://ref.atlasapi.org/channels/pressassociation.com/1959",
            "http://ref.atlasapi.org/channels/mutv",
            "http://ref.atlasapi.org/channels/skysports3hd",
            "http://ref.atlasapi.org/channels/pressassociation.com/1953",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/798",
            "http://ref.atlasapi.org/channels/motorstv",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/936",
            "http://ref.atlasapi.org/channels/skysportsf1",
            "http://ref.atlasapi.org/channels/pressassociation.com/1939",
            "http://ref.atlasapi.org/channels/pressassociation.com/1938",
            "http://ref.atlasapi.org/channels/chelseatv",
            "http://ref.atlasapi.org/channels/eurosport",
            "http://ref.atlasapi.org/channels/eurosporthd",
            "http://ref.atlasapi.org/channels/pressassociation.com/1804",
            "http://ref.atlasapi.org/channels/pressassociation.com/1805",
            "http://ref.atlasapi.org/channels/eurosport2",
            "http://ref.atlasapi.org/channels/pressassociation.com/1638",
            "http://ref.atlasapi.org/channels/skysports4",
            "http://ref.atlasapi.org/channels/skysports3",
            "http://ref.atlasapi.org/channels/skysports2",
            "http://ref.atlasapi.org/channels/skysports1",
            "http://ref.atlasapi.org/channels/skysportsnews",
            "http://ref.atlasapi.org/channels/pressassociation.com/1898",
            "http://ref.atlasapi.org/channels/pressassociation.com/1889",
            "http://ref.atlasapi.org/channels/espnhd",
            "http://ref.atlasapi.org/channels/pressassociation.com/1807",
            "http://ref.atlasapi.org/channels/pressassociation.com/1806",
            "http://ref.atlasapi.org/channels/pressassociation.com/1960",
            "http://ref.atlasapi.org/channels/pressassociation.com/1964",
            "http://ref.atlasapi.org/channels/pressassociation.com/1963",
            "http://ref.atlasapi.org/channels/pressassociation.com/1968",
            "http://ref.atlasapi.org/channels/pressassociation.com/1967",
            "http://ref.atlasapi.org/channels/pressassociation.com/1956",
            "http://ref.atlasapi.org/channels/pressassociation.com/1890",
            "http://ref.atlasapi.org/channels/pressassociation.com/1671",
            "http://ref.atlasapi.org/channels/extremesports",
            "http://ref.atlasapi.org/channels/skysports1hd",
            "http://ref.atlasapi.org/channels/skysports2hd",
            "http://ref.atlasapi.org/channels/skysportsnewshd",
            "http://ref.atlasapi.org/channels/skysports4hd",
            "http://ref.atlasapi.org/channels/setantasports1ireland",
            "http://ref.atlasapi.org/channels/pressassociation.com/1971",
            "http://ref.atlasapi.org/channels/attheraces",
            "http://ref.atlasapi.org/channels/pressassociation.com/1952",
            "http://ref.atlasapi.org/channels/pressassociation.com/1984",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/635",
            "http://ref.atlasapi.org/channels/pressassociation.com/1992",
            "http://ref.atlasapi.org/channels/pressassociation.com/2011",
            "http://ref.atlasapi.org/channels/pressassociation.com/2015",
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/1104",
            "http://ref.atlasapi.org/channels/pressassociation.com/2020",
            "http://ref.atlasapi.org/channels/pressassociation.com/2021"));

    public BaseBroadcastItemScorer(ContentResolver resolver, Score misMatchScore) {
        this.resolver = checkNotNull(resolver);
        this.misMatchScore = checkNotNull(misMatchScore);
    }

    /**
     * @inheritDoc
     */
    @Override
    public final ScoredCandidates<Item> score(Item subject, Set<? extends Item> candidates,
            ResultDescription desc) {
        Builder<Item> equivalents = DefaultScoredCandidates.fromSource(getName());

        Optional<Container> subjectContainer = getContainerIfHasTitle(subject);
        
        for (Item candidate : candidates) {
            equivalents.addEquivalent(candidate, score(subject, subjectContainer, candidate, desc));
        }

        return equivalents.build();
    }

    /**
     * Provide the unique identifier for this scorer.
     * @return the name of this scorer
     */
    protected abstract String getName();

    private Optional<Container> getContainerIfHasTitle(Item candidate) {
        Optional<Container> candidateContainer = Optional.absent();
        if (candidate.getContainer() != null) {
            candidateContainer = resolveContainerIfItHasTitle(candidate.getContainer());
        }
        return candidateContainer;
    }

    private Optional<Container> resolveContainerIfItHasTitle(ParentRef containerRef) {
        String containerUri = containerRef.getUri();
        ResolvedContent resolved = resolver.findByCanonicalUris(ImmutableList.of(containerUri));
        Maybe<Identified> possibleContainer = resolved.get(containerUri);
        if (possibleContainer.hasValue() && possibleContainer.requireValue() instanceof Container) {
            Container container = (Container)possibleContainer.requireValue();
            if (!Strings.isNullOrEmpty(container.getTitle())) {
                return Optional.of(container);
            }
        }
        return Optional.absent();
    }

    private Score score(Item subject, Optional<Container> subjectContainer, Item candidate, ResultDescription desc) {

        boolean broadcastOnSportChannel = subject.getVersions()
                .stream()
                .anyMatch(o -> o.getBroadcasts()
                        .stream()
                        .anyMatch(b -> SPORTS_CHANNEL.contains(b.getBroadcastOn())));

        if (broadcastOnSportChannel) {
            return Score.nullScore();
        }

        if (subjectAndCandidateMatch(subject, candidate)) {
            desc.appendText("%s scores %s through subject",  uriAndTitle(candidate), Score.ONE); 
            return Score.ONE;
        }
        
        if (subjectContainer.isPresent()
                && subjectContainerAndCandidateMatch(subjectContainer.get(), candidate)) {
            desc.appendText("%s scores %s through subject container %s",
                    uriAndTitle(candidate), Score.ONE, uriAndTitle(subjectContainer.get())); 
            return Score.ONE;
        }
        
        Optional<Container> candidateContainer = getContainerIfHasTitle(candidate);
        
        if (candidateContainer.isPresent()
                && subjectAndCandidateContainerMatch(subject, candidateContainer.get())) {
            desc.appendText("%s scores %s through candidate container %s",
                    uriAndTitle(candidate), Score.ONE, uriAndTitle(candidateContainer.get())); 
            return Score.ONE;
        }
        
        desc.appendText("%s scores %s, no item/container title matches",
                candidate.getCanonicalUri(), misMatchScore); 
        return misMatchScore;
    }
    
    private String uriAndTitle(Content c) {
        return String.format("'%s' (%s)", c.getTitle(), c.getCanonicalUri());
    }
    
    /**
     * Check if there is a match between the subject and candidate.
     * @param subject - the subject of the update.
     * @param candidate - the candidate being scored.
     * @return true if there is the subject and candidate match, false otherwise.
     */
    protected abstract boolean subjectAndCandidateMatch(Item subject, Item candidate);

    /**
     * Check if there is a match between the subject and candidate.
     * @param subject - the subject of the update
     * @param candidateContainer - the container of the candidate being scored.
     * @return true if there is the subject and candidate container match, false otherwise.
     */
    protected abstract boolean subjectAndCandidateContainerMatch(Item subject, Container candidateContainer);


    /**
     * Check if there is a match between the subject and candidate.
     * @param subjectContainer
     * @param candidate
     * @return true if there is the subject container and candidate container match, false otherwise.
     */
    protected abstract boolean subjectContainerAndCandidateMatch(Container subjectContainer, Item candidate);

    @Override
    public String toString() {
        return getName();
    }
    
}
