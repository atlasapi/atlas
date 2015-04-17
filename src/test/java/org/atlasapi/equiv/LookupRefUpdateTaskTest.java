package org.atlasapi.equiv;

import static org.junit.Assert.*;

import java.util.Set;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.mongo.LookupEntryTranslator;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.DateTimeZones;
import com.mongodb.DBCollection;


public class LookupRefUpdateTaskTest {

    private final DatabasedMongo db = MongoTestHelper.anEmptyTestDatabase();
    private final DBCollection lookupCollection = db.collection("lookup");
    private final DBCollection progressCollection = db.collection("progress");
    private final LookupRefUpdateTask task = new LookupRefUpdateTask(lookupCollection, progressCollection);
    private final LookupEntryTranslator translator = new LookupEntryTranslator();
    
    @Before
    public void setup() {
        lookupCollection.drop();
        progressCollection.drop();
    }
    
    @Test
    public void testSoloEntryUpdate() {
        
        Long id = 1L;
        String uri = "soloUri";
        Publisher source = Publisher.BBC;
        ContentCategory cat = ContentCategory.CHILD_ITEM;
        LookupEntry entry = entry(id, uri, source, cat);

        lookupCollection.save(translator.toDbo(entry));
        
        task.run();
        
        LookupEntry updatedEntry = translator.fromDbo(lookupCollection.findOne(uri));
        
        for (LookupRef lookupRef : refs(updatedEntry)) {
            assertEquals(id, lookupRef.id());
        }
        
    }
    
    @Test
    public void testEquialentEntryUpdate() {
        LookupEntry entry1 = entry(2L, "uri1", Publisher.BBC, ContentCategory.CHILD_ITEM);
        LookupEntry entry2 = entry(3L, "uri2", Publisher.PA, ContentCategory.CHILD_ITEM);
        
        Set<LookupRef> ref1NoId = entry1.equivalents();
        entry1 = entry1.copyWithDirectEquivalents(entry2.equivalents())
                .copyWithExplicitEquivalents(entry2.equivalents())
                .copyWithEquivalents(entry2.equivalents());
        entry2 = entry2.copyWithDirectEquivalents(ref1NoId)
                .copyWithExplicitEquivalents(ref1NoId)
                .copyWithEquivalents(ref1NoId);

        lookupCollection.save(translator.toDbo(entry1));
        lookupCollection.save(translator.toDbo(entry2));

        task.run();
        
        LookupEntry updatedEntry1 = translator.fromDbo(lookupCollection.findOne(entry1.uri()));
        LookupEntry updatedEntry2 = translator.fromDbo(lookupCollection.findOne(entry2.uri()));
        
        ImmutableMap<String, Long> ids = ImmutableMap.of(entry1.uri(), entry1.id(), entry2.uri(), entry2.id());
        
        for (LookupRef lookupRef : Iterables.concat(refs(updatedEntry1),refs(updatedEntry2))) {
            Long id = ids.get(lookupRef.uri());
            assertNotNull(id);
            assertEquals(lookupRef.uri(), id, lookupRef.id());
        }
        
    }

    private Iterable<LookupRef> refs(LookupEntry updatedEntry1) {
        return Iterables.concat(updatedEntry1.equivalents(), updatedEntry1.directEquivalents(), updatedEntry1.explicitEquivalents());
    }

    private LookupEntry entry(Long id, String uri, Publisher source, ContentCategory cat) {
        LookupRef self = new LookupRef(uri, id, source, cat);
        
        ImmutableSet<LookupRef> refs = ImmutableSet.of(new LookupRef(uri, null, source, cat));
        
        LookupEntry entry = new LookupEntry(uri, id, self, ImmutableSet.of("alias"), ImmutableSet.of(new Alias("namespace","value")), refs, refs, refs, new DateTime(DateTimeZones.UTC), new DateTime(DateTimeZones.UTC), true);
        return entry;
    }

    
}
