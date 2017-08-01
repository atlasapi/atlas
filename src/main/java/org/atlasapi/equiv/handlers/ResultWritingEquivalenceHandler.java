package org.atlasapi.equiv.handlers;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.persistence.EquivalenceResultStore;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.results.wrappers.EquivalenceResultWrapper;
import org.atlasapi.equiv.results.wrappers.EquivalentWrapper;
import org.atlasapi.equiv.results.wrappers.ScoreWrapper;
import org.atlasapi.media.entity.Content;

import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.time.DateTimeZones;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.ImmutableSet.copyOf;
import static org.atlasapi.media.entity.Identified.TO_URI;

public class ResultWritingEquivalenceHandler<T extends Content>
        implements EquivalenceResultHandler<T> {

    private static final Logger log = LoggerFactory.getLogger(ResultWritingEquivalenceHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String EQUIVALENCE_RESULTS_WRITER_ENDPOINT = Configurer.get("equiv.results.writer.endpoint").get();

    private EquivalenceResultStore store;
    private CloseableHttpClient client;

    public ResultWritingEquivalenceHandler(EquivalenceResultStore store) {
        this.store = store;
        this.client = HttpClients.createDefault();
    }

    @Override
    public boolean handle(EquivalenceResult<T> result) {

        final Ordering<Map.Entry<T, Score>> equivalenceResultOrdering = Ordering.from(
                (Comparator<Map.Entry<T, Score>>) (o1, o2) -> o1.getKey()
                        .getPublisher()
                        .compareTo(o2.getKey().getPublisher())
        )
                .compound((o1, o2) -> Score.SCORE_ORDERING.reverse()
                        .compare(o1.getValue(), o2.getValue()))
                .compound(Comparator.comparing(o -> o.getKey().getCanonicalUri()));

        T subject = result.subject();

        EquivalenceResultWrapper.Builder resultWrapperBuilder = EquivalenceResultWrapper.builder()
                .withUri(subject.getCanonicalUri())
                .withTitle(subject.getTitle());

        ImmutableSet<String> strongEquivalents = copyOf(result.strongEquivalences()
                .values()
                .stream()
                .map(ScoredCandidate.<T>toCandidate()::apply)
                .collect(Collectors.toList())
                .stream()
                .map(TO_URI::apply)
                .collect(Collectors.toList()));

        resultWrapperBuilder.withStrong(strongEquivalents);

        List<Map.Entry<T, Score>> combinedEquivalents = equivalenceResultOrdering.sortedCopy(
                result.combinedEquivalences()
                        .candidates()
                        .entrySet()
        );

        List<EquivalentWrapper> equivalentsList = Lists.newArrayList();

        for (Map.Entry<T, Score> combinedEquiv : combinedEquivalents) {
            EquivalentWrapper.Builder equivalentWrapperBuilder = EquivalentWrapper.builder();

            T content = combinedEquiv.getKey();

            equivalentWrapperBuilder.withUri(content.getCanonicalUri());
            equivalentWrapperBuilder.withTitle(content.getTitle());
            equivalentWrapperBuilder.withPublisher(content.getPublisher().key());
            equivalentWrapperBuilder.withCombined(combinedEquiv.getValue().isRealScore()
                                                  ? combinedEquiv.getValue().asDouble()
                                                  : null
            );

            List<ScoreWrapper> scoreList = Lists.newArrayList();
            for (ScoredCandidates<T> source : result.rawScores()) {
                Score sourceScore = source.candidates().get(content);

                String equivalentSource = source.source();
                Double equivalentScore = sourceScore != null && sourceScore.isRealScore()
                                         ? sourceScore.asDouble()
                                         : null;
                ScoreWrapper scoreWrapper = ScoreWrapper.create(equivalentSource, equivalentScore);

                scoreList.add(scoreWrapper);
            }

            equivalentWrapperBuilder.withScores(scoreList);

            equivalentsList.add(equivalentWrapperBuilder.build());

        }

        resultWrapperBuilder.withEquivalentsWrapper(equivalentsList);
        resultWrapperBuilder.withDescription(result.description().parts());
        resultWrapperBuilder.withTimestamp(new DateTime(DateTimeZones.UTC).toString());

        try {
            HttpPost postRequest = new HttpPost(EQUIVALENCE_RESULTS_WRITER_ENDPOINT);

            StringEntity jsonEntity = new StringEntity(MAPPER.writeValueAsString(
                    resultWrapperBuilder.build()));
            postRequest.addHeader("content-type", "application/json");
            postRequest.setEntity(jsonEntity);

            client.execute(postRequest);

        } catch (IOException e) {
            log.error(
                    "There was a problem with accessing the results equivalence writer endpoint. Message: {}",
                    Throwables.getStackTraceAsString(e)
            );
            throw Throwables.propagate(e);
        }

        store.store(result);
        return false;
    }

}
