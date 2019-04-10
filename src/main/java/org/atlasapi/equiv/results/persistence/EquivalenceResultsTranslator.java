package org.atlasapi.equiv.results.persistence;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.collect.Table;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.DateTimeZones;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.EquivalenceResults;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;

public class EquivalenceResultsTranslator {
    private static final SubstitutionTableNumberCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private static final String TIMESTAMP = "timestamp";
    private static final String TITLE = "title";
    private static final String EQUIVS = "equivs";
    private static final String STRONG = "strong";
    private static final String RESULTS = "results";

    private static final String COMBINED = "combined";
    private static final String PUBLISHER = "publisher";
    private static final String SCORES = "scores";
    private static final String SOURCE = "source";
    private static final String SCORE = "score";
    private static final String DESC = "desc";

    private static final String AID = "aid";


    public <T extends Content> DBObject toDBObject(EquivalenceResults<T> results) {
        DBObject dbo = new BasicDBObject();

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

        T target = results.subject();
        TranslatorUtils.from(dbo, ID, target.getCanonicalUri());
        TranslatorUtils.from(dbo, AID, target.getId());
        TranslatorUtils.from(dbo, TITLE, target.getTitle());
        TranslatorUtils.from(dbo, PUBLISHER, target.getPublisher().title());

        BasicDBList resultList = new BasicDBList();

        for(EquivalenceResult<T> result : results.getResults()) {
            DBObject resultDbo = new BasicDBObject();

            TranslatorUtils.fromSet(
                    resultDbo,
                    result.strongEquivalences().values().stream()
                            .map(ScoredCandidate::candidate)
                            .map(T::getId)
                            .map(BigInteger::valueOf)
                            .map(codec::encode)
                            .collect(MoreCollectors.toImmutableSet()),
                    STRONG
            );

            BasicDBList equivList = new BasicDBList();

            for (Entry<T, Score> combinedEquiv : equivalenceResultOrdering.sortedCopy(result.combinedEquivalences().candidates().entrySet())) {
                DBObject equivDbo = new BasicDBObject();

                T content = combinedEquiv.getKey();

                TranslatorUtils.from(equivDbo, ID, content.getCanonicalUri());
                TranslatorUtils.from(equivDbo, AID, content.getId());
                TranslatorUtils.from(equivDbo, TITLE, content.getTitle());
                TranslatorUtils.from(equivDbo, PUBLISHER, content.getPublisher().key());
                TranslatorUtils.from(equivDbo, COMBINED, combinedEquiv.getValue().isRealScore() ? combinedEquiv.getValue().asDouble() : null);

                BasicDBList scoreList = new BasicDBList();
                for (ScoredCandidates<T> source : result.rawScores()) {
                    Score sourceScore = source.candidates().get(content);

                    BasicDBObject scoreDbo = new BasicDBObject();
                    scoreDbo.put(SOURCE, source.source());
                    scoreDbo.put(SCORE, sourceScore != null && sourceScore.isRealScore() ? sourceScore.asDouble() : null);

                    scoreList.add(scoreDbo);
                }
                TranslatorUtils.from(equivDbo, SCORES, scoreList);

                equivList.add(equivDbo);
            }
            TranslatorUtils.from(resultDbo, EQUIVS, equivList);
            resultDbo.put(DESC, result.description().parts());
            resultList.add(resultDbo);
        }
        TranslatorUtils.from(dbo, RESULTS, resultList);

        TranslatorUtils.fromDateTime(dbo, TIMESTAMP, new DateTime(DateTimeZones.UTC));

        dbo.put(DESC, results.description().parts());

        return dbo;
    }

    @Nullable
    public StoredEquivalenceResults fromDBObject(DBObject dbo) {
        if(dbo == null) {
            return null;
        }

        if(!dbo.containsField(RESULTS)) { //New field may not exist e.g. objects created by old translator
            return null;
        }
        
        String targetId = TranslatorUtils.toString(dbo, ID);
        Long targetAid = TranslatorUtils.toLong(dbo, AID);
        String targetTitle = TranslatorUtils.toString(dbo, TITLE);
        String targetPublisher = TranslatorUtils.toString(dbo, PUBLISHER);


        List<StoredEquivalenceResultTable> resultTables = new ArrayList<>();

        for (DBObject resultDbo : TranslatorUtils.toDBObjectList(dbo, RESULTS)) {
            Set<String> strongs = TranslatorUtils.toSet(resultDbo, STRONG);

            ImmutableList.Builder<CombinedEquivalenceScore> totals = ImmutableList.builder();
            Table<String, String, Double> results = HashBasedTable.create();

            for (DBObject equivDbo : TranslatorUtils.toDBObjectList(resultDbo, EQUIVS)) {
                String id = TranslatorUtils.toString(equivDbo, ID);
                Long aid = TranslatorUtils.toLong(equivDbo, AID);
                String encodedAid = codec.encode(BigInteger.valueOf(aid));
                Double combined = equivDbo.containsField(COMBINED) ? TranslatorUtils.toDouble(equivDbo, COMBINED) : Double.NaN;
                totals.add(
                        new CombinedEquivalenceScore(encodedAid, TranslatorUtils.toString(equivDbo, TITLE), combined, strongs.contains(encodedAid), publisherName(equivDbo))
                );
                for (DBObject scoreDbo : TranslatorUtils.toDBObjectList(equivDbo, SCORES)) {
                    Double score = TranslatorUtils.toDouble(scoreDbo, SCORE);
                    results.put(encodedAid, TranslatorUtils.toString(scoreDbo, SOURCE), score == null ? Double.NaN : score);
                }
            }

            resultTables.add(new StoredEquivalenceResultTable(results, totals.build(), (List<Object>)resultDbo.get(DESC)));
        }
        
        @SuppressWarnings("unchecked") List<Object> description = (List<Object>)dbo.get(DESC);
        return new StoredEquivalenceResults(
                targetId,
                codec.encode(BigInteger.valueOf(targetAid)),
                targetTitle,
                targetPublisher,
                resultTables,
                TranslatorUtils.toDateTime(dbo, TIMESTAMP),
                description
        );
    }
    
    private String publisherName(DBObject equivDbo) {
        Maybe<Publisher> restoredPublisher = Publisher.fromKey(TranslatorUtils.toString(equivDbo, PUBLISHER));
        String publisher = restoredPublisher.hasValue() ? restoredPublisher.requireValue().title() : "Unknown Publisher";
        return publisher;
    }
    
}
