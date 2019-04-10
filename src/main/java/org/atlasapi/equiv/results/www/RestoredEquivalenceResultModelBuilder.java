package org.atlasapi.equiv.results.www;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metabroadcast.common.model.SimpleModel;
import com.metabroadcast.common.model.SimpleModelList;
import com.metabroadcast.common.time.DateTimeZones;
import org.atlasapi.equiv.results.persistence.CombinedEquivalenceScore;
import org.atlasapi.equiv.results.persistence.StoredEquivalenceResultTable;
import org.atlasapi.equiv.results.persistence.StoredEquivalenceResults;
import org.atlasapi.equiv.results.probe.EquivalenceResultProbe;
import org.eclipse.jetty.util.UrlEncoded;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class RestoredEquivalenceResultModelBuilder {

    public SimpleModel build(StoredEquivalenceResults target, EquivalenceResultProbe probe) {
        SimpleModel model = new SimpleModel();
        
        model.put("id", target.id());
        model.put("encodedId", UrlEncoded.encodeString(target.id()));
        model.put("aid", target.getAid());
        model.put("publisher", target.getPublisher());
        model.put("title", target.title());
        model.put("time", target.resultTime().toDateTime(DateTimeZones.LONDON).toString("YYYY-MM-dd HH:mm:ss"));
        
        boolean hasStrong = false;

        int equivalencesCount = 0;

        SimpleModelList resultTables = new SimpleModelList();

        for (StoredEquivalenceResultTable resultTable : target.getResultTables()) {
            SimpleModel resultTableModel = new SimpleModel();
            Map<String, Double> totals = Maps.newHashMap();
            totals.put("combined", null);
            for (String source : resultTable.sourceResults().columnKeySet()) {
                totals.put(source, null);
            }

            SimpleModelList equivalences = new SimpleModelList();
            for (CombinedEquivalenceScore equivalence : resultTable.combinedResults()) {
                SimpleModel equivModel = new SimpleModel();

                equivModel.put("id",equivalence.id());
                equivModel.put("encodedId",UrlEncoded.encodeString(equivalence.id()));
                equivModel.put("title", equivalence.title());
                equivModel.put("strong", equivalence.strong());
                equivModel.put("publisher", equivalence.publisher());
                equivModel.put("scores", scores(equivalence.score(), resultTable.sourceResults().row(equivalence.id()), totals));

                hasStrong |= equivalence.strong();
                equivalencesCount++;

                equivModel.put("expected", expected(equivalence, probe));

                equivalences.add(equivModel);
            }
            resultTableModel.put("totals", model(totals));

            resultTableModel.put("equivalences", equivalences);
            resultTableModel.putStrings("sources", resultTable.sourceResults().columnKeySet());

            if (resultTable.description() != null) {
                resultTableModel.put("desc", modelDesc(resultTable.description()));
            }

            resultTables.add(resultTableModel);
        }

        model.put("resultTables", resultTables);

        model.put("equivalences", equivalencesCount);

        model.put("hasStrong", hasStrong);

        if (target.description() != null) {
            model.put("desc", modelDesc(target.description()));
        }
        return model;
    }

    private Collection<SimpleModel> modelDesc(List<Object> description) {
        return Lists.transform(description, new Function<Object, SimpleModel>() {
            @Override
            public SimpleModel apply(Object input) {
                boolean isList = input instanceof List;
                SimpleModel model = new SimpleModel().put("type", isList ? "list" : "string");
                if (isList) {
                    model.put("value", modelDesc((List<Object>)input));
                } else {
                    model.put("value", (String) input);
                }
                return model;
            }
        });
    }

    private SimpleModel model(Map<String, Double> totals) {
        SimpleModel model = new SimpleModel();
        for (Entry<String, Double> totalScore : totals.entrySet()) {
            if(totalScore.getValue() != null) {
                model.put(totalScore.getKey(), totalScore.getValue());
            } else {
                model.put(totalScore.getKey(), false);
            }
        }
        return model;
    }

    private String expected(CombinedEquivalenceScore key, EquivalenceResultProbe probe) {
        if (probe != null) {
            if (probe.expectedEquivalent().contains(key.id())) {
                return "expected";
            }
            if (probe.expectedNotEquivalent().contains(key.id())) {
                return "notexpected";
            }
        }
        return "unknown";
    }

    private SimpleModel scores(Double combined, Map<String, Double> sourceScores, Map<String, Double> totals) {
        
        SimpleModel scoreModel = new SimpleModel();
        if(combined.isNaN()) {
            scoreModel.put("combined", false);
        } else {
            scoreModel.put("combined", combined);
        }
        
        if(combined != null && !combined.isNaN() && !(combined < 0)) {
            Double runningTotal = totals.get("combined");
            totals.put("combined", runningTotal == null ? combined : combined + runningTotal);
        }
        
        for (Entry<String, Double> sourceScore : sourceScores.entrySet()) {
            String source = sourceScore.getKey();
            Double score = sourceScore.getValue();
            
            if(score == null || score.isNaN()) {
                scoreModel.put(source, false);
            } else {
                scoreModel.put(source, score);
            }
            
            if(score != null && !score.isNaN() && score > 0) {
                Double sourceTotal = totals.get(source);
                totals.put(source, sourceTotal == null ? score : score + sourceTotal);
            }
        }
        return scoreModel;
    }
    
}
