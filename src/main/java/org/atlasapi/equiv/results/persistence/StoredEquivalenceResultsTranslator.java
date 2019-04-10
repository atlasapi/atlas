package org.atlasapi.equiv.results.persistence;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.collect.Table;
import com.metabroadcast.common.time.DateTimeZones;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.EquivalenceResults;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Content;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.collect.Iterables.transform;
import static org.atlasapi.media.entity.Identified.TO_URI;

public class StoredEquivalenceResultsTranslator {

    public <T extends Content> StoredEquivalenceResults toStoredEquivalenceResults(EquivalenceResults<T> results) {

        
        final Ordering<Entry<T, Score>> equivalenceResultOrdering = Ordering.from(new Comparator<Entry<T, Score>>() {
            @Override
            public int compare(Entry<T, Score> o1, Entry<T, Score> o2) {
                return o1.getKey().getPublisher().compareTo(o2.getKey().getPublisher());
            }
        }).compound(new Comparator<Entry<T, Score>>() {
            @Override
            public int compare(Entry<T, Score> o1, Entry<T, Score> o2) {
                return Score.SCORE_ORDERING.reverse().compare(o1.getValue(), o2.getValue());
            }
        }).compound(new Comparator<Entry<T, Score>>() {
            @Override
            public int compare(Entry<T, Score> o1, Entry<T, Score> o2) {
                return o1.getKey().getCanonicalUri().compareTo(o2.getKey().getCanonicalUri());
            }
        });

        List<StoredEquivalenceResultTable> resultTables = new ArrayList<>();

        for(EquivalenceResult<T> result : results.getResults()) {
            ImmutableList.Builder<CombinedEquivalenceScore> totals = ImmutableList.builder();
            Table<String, String, Double> resultTable = HashBasedTable.create();

            Set<String> strongEquivalences = copyOf(transform(transform(result.strongEquivalences().values(), ScoredCandidate.<T>toCandidate()), TO_URI));

            for (Entry<T, Score> combinedEquiv : equivalenceResultOrdering.sortedCopy(result.combinedEquivalences().candidates().entrySet())) {

                T content = combinedEquiv.getKey();

                Double combinedScore = combinedEquiv.getValue().isRealScore() ? combinedEquiv.getValue().asDouble() : Double.NaN;
                totals.add(new CombinedEquivalenceScore(content.getCanonicalUri(), content.getTitle(), combinedScore, strongEquivalences.contains(content.getCanonicalUri()), content.getPublisher().title()));
                for (ScoredCandidates<T> source : result.rawScores()) {

                    Score sourceScore = source.candidates().get(content);
                    Double score = sourceScore != null && sourceScore.isRealScore() ? sourceScore.asDouble() : Double.NaN;
                    resultTable.put(content.getCanonicalUri(), source.source(), score);
                }

            }
            resultTables.add(new StoredEquivalenceResultTable(resultTable, totals.build(), result.description().parts()));
        }
        
        return new StoredEquivalenceResults(
                results.subject().getCanonicalUri(),
                results.subject().getTitle(),
                resultTables,
                new DateTime(DateTimeZones.UTC),
                results.description().parts()
        );
    }
}
