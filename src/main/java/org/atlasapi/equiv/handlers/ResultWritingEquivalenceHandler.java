package org.atlasapi.equiv.handlers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.persistence.EquivalenceResultStore;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Content;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.base.Throwables;
import com.google.common.collect.Ordering;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.util.SerializationUtils;

import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.collect.Iterables.transform;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static org.atlasapi.media.entity.Identified.TO_URI;

public class ResultWritingEquivalenceHandler<T extends Content>
        implements EquivalenceResultHandler<T> {

    private EquivalenceResultStore store;

    public ResultWritingEquivalenceHandler(EquivalenceResultStore store) {
        this.store = store;
    }

    @Override
    public boolean handle(EquivalenceResult<T> result) {

        final Ordering<Map.Entry<T, Score>> equivalenceResultOrdering = Ordering.from(
                (Comparator<Map.Entry<T, Score>>) (o1, o2) -> o1.getKey().getPublisher().compareTo(o2.getKey().getPublisher())
        )
                .compound((o1, o2) -> Score.SCORE_ORDERING.reverse().compare(o1.getValue(), o2.getValue()))
                .compound(Comparator.comparing(o -> o.getKey().getCanonicalUri()));

        T subject = result.subject();

        JSONObject jsonObject = new JSONObject();

        jsonObject.put("uri", subject.getCanonicalUri());
        jsonObject.put("title", subject.getTitle());

        JSONArray strongArray = new JSONArray();
        strongArray.addAll(copyOf(result.strongEquivalences()
                .values()
                .stream()
                .map(ScoredCandidate.<T>toCandidate()::apply)
                .collect(Collectors.toList())
                .stream()
                .map(TO_URI::apply)
                .collect(Collectors.toList())));
        jsonObject.put("strong", strongArray);

        JSONArray equivList = new JSONArray();

        for (Map.Entry<T, Score> combinedEquiv : equivalenceResultOrdering.sortedCopy(result.combinedEquivalences().candidates().entrySet())) {
            JSONObject equivDbo = new JSONObject();

            T content = combinedEquiv.getKey();

            equivDbo.put("uri", content.getCanonicalUri());
            equivDbo.put("title", content.getTitle());
            equivDbo.put("publisher", content.getPublisher().key());
            equivDbo.put("combined", combinedEquiv.getValue().isRealScore() ? combinedEquiv.getValue().asDouble() : null);

            JSONArray scoreList = new JSONArray();
            for (ScoredCandidates<T> source : result.rawScores()) {
                Score sourceScore = source.candidates().get(content);

                JSONObject scoreDbo = new JSONObject();

                scoreDbo.put("source", source.source());
                scoreDbo.put("score", sourceScore != null && sourceScore.isRealScore() ? sourceScore.asDouble() : null);

                scoreList.add(scoreDbo);
            }

            equivDbo.put("scores", scoreList);

            equivList.add(equivDbo);
        }

        jsonObject.put("description", result.description().parts().toString());

        jsonObject.put("timestamp", new DateTime(DateTimeZones.UTC).toString());

        System.out.println(jsonObject.toJSONString());

        try {
            CloseableHttpClient client = HttpClients.createDefault();

            HttpPost postRequest = new HttpPost("http://equivalence-writer-task-master.stage.svc.cluster.local/equivalence/result");

            StringEntity jsonEntity = new StringEntity(jsonObject.toJSONString());
            postRequest.addHeader("content-type", "application/json");
            postRequest.setEntity(jsonEntity);

            client.execute(postRequest);

        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        store.store(result);
        return false;
    }

}
