package org.atlasapi.equiv;

import static com.metabroadcast.common.scheduling.UpdateProgress.FAILURE;
import static com.metabroadcast.common.scheduling.UpdateProgress.SUCCESS;
import static org.atlasapi.persistence.content.ContentCategory.CHILD_ITEM;
import static org.atlasapi.persistence.content.ContentCategory.TOP_LEVEL_ITEM;
import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;
import static org.atlasapi.persistence.content.listing.ContentListingProgress.progressFrom;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.atlasapi.equiv.update.tasks.ScheduleTaskProgressStore;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.content.mongo.MongoContentTables;
import org.atlasapi.persistence.media.entity.ChildRefTranslator;
import org.atlasapi.persistence.media.entity.ContentGroupTranslator;
import org.atlasapi.persistence.media.entity.CrewMemberTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class PersonRefUpdateTask extends ScheduledTask {
    
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final ContentLister contentStore;
    private final ScheduleTaskProgressStore progressStore;
    
    private final DBCollection standalone;
    private final DBCollection children;
    private final DBCollection people;

    private ImmutableList<Publisher> publishers;
    private String scheduleKey;
    
    private final CrewMemberTranslator crewMemberTranslator = new CrewMemberTranslator();

    private static final String PERSON_CHILD_REF_URI_KEY = ContentGroupTranslator.CONTENT_URIS_KEY + "." + ChildRefTranslator.URI_KEY;
    private static final String PERSON_CHILD_REF_ID_UPDATE_KEY = ContentGroupTranslator.CONTENT_URIS_KEY + ".$." + ChildRefTranslator.ID_KEY;

    public PersonRefUpdateTask(ContentLister lister, DatabasedMongo mongo, ScheduleTaskProgressStore progressStore) {
        this.contentStore = lister;
        
        MongoContentTables contentTables = new MongoContentTables(mongo);
        children = contentTables.collectionFor(ContentCategory.CHILD_ITEM);
        standalone = contentTables.collectionFor(ContentCategory.TOP_LEVEL_ITEM);
        people = mongo.collection("people");
        
        this.progressStore = progressStore;
    }
    
    public PersonRefUpdateTask forPublishers(Publisher... publishers) {
        this.publishers = ImmutableList.copyOf(publishers);
        this.scheduleKey = "personref" + Joiner.on("-").join(this.publishers);
        return this;
    }
    
    @Override
    protected void runTask() {
        
        ContentListingProgress progress = progressStore.progressForTask(scheduleKey);
        log.info("Started: {} from {}", scheduleKey, startProgress(progress));
        
        Map<String, Long> peopleIds = getPeopleIds();
        log.info("People ID map has {} entries", peopleIds.size());
        
        Iterator<Content> children = contentStore.listContent(defaultCriteria()
                .forPublishers(publishers)
                .forContent(ImmutableList.of(TOP_LEVEL_ITEM, CHILD_ITEM))
                .startingAt(progress)
                .build());

        UpdateProgress processed = UpdateProgress.START;
        Content content = null;

        try {
            while (children.hasNext() && shouldContinue()) {
                try {
                    content = children.next();
                    updatePeopleReferences((Item) content, peopleIds);
                    reportStatus(String.format("%s. Processing %s", processed, content));
                    processed = processed.reduce(SUCCESS);
                    if (processed.getTotalProgress() % 100 == 0) {
                        updateProgress(progressFrom(content));
                    }
                } catch (Exception e) {
                    processed = processed.reduce(FAILURE);
                    log.error("ChildRef update failed: " + content, e);
                }
            }
        } catch (Exception e) {
            log.error("Exception running task " + scheduleKey, e);
            persistProgress(false, content);
            throw Throwables.propagate(e);
        }
        reportStatus(processed.toString());
        persistProgress(shouldContinue(), content);
    }
 
    
    private void updatePeopleReferences(Item item, Map<String, Long> peopleIds) {
        if (item.getPeople().isEmpty()) {
            return;
        }
        ChildRef childRef = item.childRef();
        for (CrewMember crew : item.getPeople()) {
            if (crew.getCanonicalUri() != null) {
                Long personId = peopleIds.get(crew.getCanonicalUri());
                if (personId != null) {
                    addRefToPerson(crew.getCanonicalUri(), childRef);
                    crew.setId(personId);
                }
            }
        }
        DBCollection coll = item.getContainer() == null ? standalone
                                                        : children;
        coll.update(
            new BasicDBObject(MongoConstants.ID, item.getCanonicalUri()),
            new BasicDBObject(MongoConstants.SET, toDbos(item.getPeople())),
            MongoConstants.NO_UPSERT,
            MongoConstants.SINGLE
        );
    }

    private Map<String, Long> getPeopleIds() {
        DBCursor peopleIds = this.people.find(
            new BasicDBObject(),
            new BasicDBObject(ImmutableMap.of(
                MongoConstants.ID,1,
                IdentifiedTranslator.OPAQUE_ID,1)
            )
        );
        Map<String,Long> idMap = Maps.newHashMap();
        for (DBObject dbObject : peopleIds) {
            if (dbObject.get(MongoConstants.ID) instanceof String) {
                idMap.put(
                    TranslatorUtils.toString(dbObject, MongoConstants.ID),
                    TranslatorUtils.toLong(dbObject, IdentifiedTranslator.OPAQUE_ID)
                );
            }
        }
        return idMap;
    }

    private void addRefToPerson(String personUri, ChildRef childRef) {
        people.update(
            new BasicDBObject(ImmutableMap.<String, Object>of(
                MongoConstants.ID, personUri,
                PERSON_CHILD_REF_URI_KEY, childRef.getUri()
            )), 
            new BasicDBObject(MongoConstants.SET, 
                new BasicDBObject(PERSON_CHILD_REF_ID_UPDATE_KEY, childRef.getId())
            ),
            MongoConstants.NO_UPSERT,
            MongoConstants.SINGLE
        );
    }

    private BasicDBObject toDbos(List<CrewMember> people) {
        BasicDBList peopleDbos = new BasicDBList();
        for (CrewMember member : people) {
            peopleDbos.add(crewMemberTranslator.toDBObject(new BasicDBObject(), member));
        }
        return new BasicDBObject("people", peopleDbos);
    }

    public void updateProgress(ContentListingProgress progress) {
        progressStore.storeProgress(scheduleKey, progress);
    }
    
    private void persistProgress(boolean finished, Content content) {
        if (finished) {
            updateProgress(ContentListingProgress.START);
            log.info("Finished: {}", scheduleKey);
        } else {
            if (content != null) {
                updateProgress(progressFrom(content));
                log.info("Stopped: {} at {}", scheduleKey, content.getCanonicalUri());
            }
        }
    }

    private String startProgress(ContentListingProgress progress) {
        if (progress == null || ContentListingProgress.START.equals(progress)) {
            return "start";
        }
        return String.format("%s %s %s", progress.getCategory(), progress.getPublisher(), progress.getUri());
    }
}
