package org.atlasapi.equiv.results.persistence;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.atlasapi.equiv.results.EquivalenceResults;
import org.atlasapi.media.entity.Content;

import java.util.List;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;

public class MongoEquivalenceResultStore implements EquivalenceResultStore {

    private DBCollection equivResults;
    private EquivalenceResultsTranslator translator = new EquivalenceResultsTranslator();
    private EquivalenceResultTranslator oldTranslator = new EquivalenceResultTranslator();

    public MongoEquivalenceResultStore(DatabasedMongo mongo) {
        this.equivResults = mongo.collection("equivalence");
    }
    
    @Override
    public <T extends Content> StoredEquivalenceResults store(EquivalenceResults<T> results) {
        DBObject dbo = translator.toDBObject(results);
        equivResults.update(where().fieldEquals(ID, results.subject().getCanonicalUri()).build(), dbo, true, false);
        return fromDBObject(dbo);
    }

    @Override
    public StoredEquivalenceResults forId(String canonicalUri) {
        return fromDBObject(equivResults.findOne(canonicalUri));
    }

    @Override
    public List<StoredEquivalenceResults> forIds(Iterable<String> canonicalUris) {
        return ImmutableList.copyOf(Iterables.transform(equivResults.find(where().idIn(canonicalUris).build()), new Function<DBObject, StoredEquivalenceResults>() {

            @Override
            public StoredEquivalenceResults apply(DBObject input) {
                return fromDBObject(input);
            }
        }));
    }

    private StoredEquivalenceResults fromDBObject(DBObject dbo) {
        StoredEquivalenceResults results = translator.fromDBObject(dbo);
        if (results == null) {
            results = new StoredEquivalenceResults(oldTranslator.fromDBObject(dbo));
        }
        return results;

    }

}
