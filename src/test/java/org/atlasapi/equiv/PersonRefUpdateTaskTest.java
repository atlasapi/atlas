package org.atlasapi.equiv;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.atlasapi.equiv.update.tasks.MongoScheduleTaskProgressStore;
import org.atlasapi.equiv.update.tasks.ScheduleTaskProgressStore;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.LookupResolvingContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentTables;
import org.atlasapi.persistence.content.mongo.MongoContentWriter;
import org.atlasapi.persistence.content.mongo.MongoPersonStore;
import org.atlasapi.persistence.content.people.PersonStore;
import org.atlasapi.persistence.lookup.TransitiveLookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.atlasapi.persistence.player.PlayerResolver;
import org.atlasapi.persistence.service.ServiceResolver;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.ReadPreference;

public class PersonRefUpdateTaskTest {
    
    private final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
    private final PersistenceAuditLog persistenceAuditLog = new NoLoggingPersistenceAuditLog();

    private final ScheduleTaskProgressStore progressStore = new MongoScheduleTaskProgressStore(mongo);
    private final MongoLookupEntryStore contentLookup = new MongoLookupEntryStore(mongo.collection("lookup"), 
            persistenceAuditLog, ReadPreference.primary());
    private final ContentLister lister = new MongoContentLister(mongo);

    private final ServiceResolver serviceResolver = mock(ServiceResolver.class);
    private final PlayerResolver playerResolver = mock(PlayerResolver.class);
    
    private final PersonRefUpdateTask updateTask = new PersonRefUpdateTask(lister, mongo, progressStore)
        .forPublishers(Publisher.BBC);
    
    private final ContentWriter contentWriter = new MongoContentWriter(mongo, contentLookup, persistenceAuditLog, playerResolver, serviceResolver, new SystemClock());
    private final ContentResolver contentResolver = new LookupResolvingContentResolver(new MongoContentResolver(mongo, contentLookup), contentLookup);

    private final LookupEntryStore peopleLookup = new MongoLookupEntryStore(mongo.collection("peopleLookup"), 
            persistenceAuditLog, ReadPreference.primary());
    private final PersonStore personStore = new MongoPersonStore(mongo, TransitiveLookupWriter.explicitTransitiveLookupWriter(peopleLookup), peopleLookup, persistenceAuditLog);

    private Item item1;
    private Item item2;
    private Person person;
    
    @Before
    public void setup() {
        CrewMember crewMember = CrewMember.crewMember("cm", "Jim", "director", Publisher.BBC);
        item1 = new Item("uri1","curie1",Publisher.BBC);
        item1.addPerson(crewMember);
        
        Brand brand = new Brand("parent", "pcurie", Publisher.BBC);
        contentWriter.createOrUpdate(brand);
        item2 = new Item("uri2","curie2",Publisher.BBC);
        item2.addPerson(crewMember);
        item2.setParentRef(new ParentRef("parent"));
        
        contentWriter.createOrUpdate(item1);
        contentWriter.createOrUpdate(item2);
        
        person = crewMember.toPerson();
        person.setContents(ImmutableList.<ChildRef>builder()
                .addAll(person.getContents())
                .add(item1.childRef())
                .add(item2.childRef())
                .build());
        personStore.createOrUpdatePerson(person);
        personStore.updatePersonItems(person);
        
        MongoContentTables tables = new MongoContentTables(mongo);
        updateId(tables.collectionFor(ContentCategory.TOP_LEVEL_ITEM), item1.getCanonicalUri(), new Long(1));
        updateId(tables.collectionFor(ContentCategory.CHILD_ITEM), item2.getCanonicalUri(), new Long(2));
        updateId(mongo.collection("lookup"), item1.getCanonicalUri(), new Long(1));
        updateId(mongo.collection("lookup"), item2.getCanonicalUri(), new Long(2));
        
        mongo.collection("people").update(
            new BasicDBObject(MongoConstants.ID, person.getCanonicalUri()),
            new BasicDBObject(MongoConstants.SET, 
                new BasicDBObject(IdentifiedTranslator.OPAQUE_ID, new Long(3))
            )
        );
        
    }

    private void updateId(DBCollection coll, String uri, Long id) {
        coll.update(
            new BasicDBObject(MongoConstants.ID, uri),
            new BasicDBObject(MongoConstants.SET, 
                new BasicDBObject(IdentifiedTranslator.OPAQUE_ID, id)
            )
        );
    }
    
    @Test
    public void test() {
        updateTask.run();
        
        ResolvedContent resolved = contentResolver.findByCanonicalUris(ImmutableList.of(item1.getCanonicalUri()));
        
        Maybe<Identified> possibleItem = resolved.get(item1.getCanonicalUri());
        Item item1 = (Item) possibleItem.requireValue();
        CrewMember member = Iterables.getOnlyElement(item1.getPeople());
        assertThat(member.getId(), is(3L));
        
        resolved = contentResolver.findByCanonicalUris(ImmutableList.of(item2.getCanonicalUri()));
        
        possibleItem = resolved.get(item2.getCanonicalUri());
        Item item2 = (Item) possibleItem.requireValue();
        member = Iterables.getOnlyElement(item2.getPeople());
        assertThat(member.getId(), is(3L));
        
        Optional<Person> resolvedPerson = personStore.person(person.getCanonicalUri());
        List<ChildRef> itemRefs = resolvedPerson.get().getContents();
        assertThat(itemRefs.get(0).getId(), is(item1.getId()));
        assertThat(itemRefs.get(1).getId(), is(item2.getId()));
        
    }

}
