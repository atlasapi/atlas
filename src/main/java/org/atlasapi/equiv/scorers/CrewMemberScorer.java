package org.atlasapi.equiv.scorers;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.CrewMember.Role;
import org.atlasapi.media.entity.Item;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Set;

import static java.math.BigDecimal.ROUND_HALF_UP;
import static java.math.RoundingMode.HALF_UP;

public class CrewMemberScorer implements EquivalenceScorer<Item> {

    private Function<Item, List<CrewMember>> peopleExtractor;

    public CrewMemberScorer(Function<Item, List<CrewMember>> peopleExtractor) {
        this.peopleExtractor = peopleExtractor;
    }
    
    public CrewMemberScorer() {
        this(new Function<Item, List<CrewMember>>() {
            @Override
            public List<CrewMember> apply(Item input) {
                return input.getPeople();
            }
        });
    }
    
    
    @Override
    public ScoredCandidates<Item> score(
            Item content,
            Set<? extends Item> candidates,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        EquivToTelescopeComponent scorerComponent = EquivToTelescopeComponent.create();
        scorerComponent.setComponentName("Crew Member Scorer");

        Builder<Item> scored = DefaultScoredCandidates.fromSource("crew");

        List<CrewMember> contentCrew = peopleExtractor.apply(content);
        
        if (nullOrEmpty(contentCrew)) {
            desc.appendText("Subject has no crew");
            for (Item candidate : candidates) {
                scored.addEquivalent(candidate, Score.NULL_SCORE);
                if (candidate.getId() != null) {
                    scorerComponent.addComponentResult(
                            candidate.getId(),
                            ""
                    );
                }
            }
            return scored.build();
        }
        
        for (Item candidate : candidates) {
            List<CrewMember> candidateCrew = peopleExtractor.apply(candidate);
            Score score;
            if (nullOrEmpty(candidateCrew)) {
                desc.appendText("%s has no crew", candidate.getCanonicalUri());
                score = Score.NULL_SCORE;
            } else {
                desc.startStage(candidate.getCanonicalUri()+":");
                score = score(contentCrew, candidateCrew, desc);
                desc.finishStage();
            }
            scored.addEquivalent(candidate, score);

            if (candidate.getId() != null) {
                scorerComponent.addComponentResult(
                        candidate.getId(),
                        String.valueOf(score.asDouble())
                );
            }
        }
        return scored.build();
    }

    private Score score(List<CrewMember> contentCrew, List<CrewMember> candidateCrew, ResultDescription desc) {
        //consistently choose needles and haystack so the scoring will be symmetric.
        int contentSize = contentCrew.size();
        int candidateSize = candidateCrew.size();
        List<CrewMember> needles = contentSize < candidateSize ? contentCrew : candidateCrew;
        List<CrewMember> haystack = contentSize < candidateSize ? candidateCrew : contentCrew;

        /*
         * This is obviously contentSize * candidateSize. Since there usually
         * aren't many contributors this shouldn't be a problem. If it is then
         * we can sort by name and go from there, assuming matching is binary.
         * 
         * This could be the sum of the best score for each needle in the
         * haystack allowing in-exact matches between people. Again n^2
         */
        int found = 0;
        for (CrewMember needle : needles) {
            if (needle.name() != null) {
                CrewMember match = needleInHaystack(needle, haystack);
                if (match != null) {
                    desc.appendText(describeMatch(needle, match));
                    found++;
                } else {
                    desc.appendText("no match: %s (%s)", needle.getCanonicalUri(), nameAndRole(needle));
                }
            } else {
                desc.appendText("no name: %s", needle.getCanonicalUri());
            }
        }
        
        double score = score(found, needles.size());
        
        desc.appendText("matched: %s/%s -> %s", found, needles.size(), scoreFormat.format(score));
        return Score.valueOf(score);
    }

    private static final DecimalFormat scoreFormat = new DecimalFormat("#.###");
    {
        scoreFormat.setMinimumFractionDigits(1);
        scoreFormat.setPositivePrefix("+");
        scoreFormat.setNegativePrefix("-");
    }
    
    private static final BigDecimal A = BigDecimal.valueOf(-40)
            .divide(BigDecimal.valueOf(21), 3, HALF_UP);
    private static final BigDecimal B = BigDecimal.valueOf(82)
            .divide(BigDecimal.valueOf(21), 3, HALF_UP);
    private static final BigDecimal C = BigDecimal.valueOf(-1);
    
    // y = (-40/21)x^2 + (82/21)x - 1 gives:
    // 1 for exact matches
    // 0 for 30% matches
    // -1 for no matches
    private double score(int matched, int maxMatched) {
        BigDecimal x = BigDecimal.valueOf(matched)
                .divide(BigDecimal.valueOf(maxMatched),3, HALF_UP);
        BigDecimal r = A.multiply(x.multiply(x))
                .add(B.multiply(x))
                .add(C)
                .setScale(3, ROUND_HALF_UP);
        return Math.max(-1.0, Math.min(1.0, r.doubleValue()));
    }

    private CrewMember needleInHaystack(CrewMember needle, List<CrewMember> haystack) {
        for (CrewMember hay : haystack) {
            if (Objects.equal(normalize(needle.name()), normalize(hay.name()))
             && Objects.equal(needle.role(), hay.role())) {
                return hay;
            }
        }
        return null;
    }
    
    private String normalize(String name) {
        return name.toLowerCase().replaceAll("[^\\d\\w\\s]","");
    }

    private String describeMatch(CrewMember needle, CrewMember match) {
        return String.format("%s (%s) matched %s (%s)", 
            needle.getCanonicalUri(), nameAndRole(needle),
            match.getCanonicalUri(), nameAndRole(match)
        );
    }
    
    private String nameAndRole(CrewMember member) {
        Role role = member.role();
        String roleKey = role == null ? "" : "," + role.key();
        return String.format("%s%s", normalize(member.name()), roleKey);
    }

    private boolean nullOrEmpty(List<CrewMember> people) {
        return people == null || people.isEmpty();
    }

    @Override
    public String toString() {
        return "Crew Member";
    }
    
}
