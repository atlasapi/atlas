package org.atlasapi.equiv;

import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.mongo.DatabasedMongoClient;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.lookup.entry.EquivRefs;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.mongo.LookupEntryTranslator;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.scheduling.UpdateProgress.FAILURE;
import static com.metabroadcast.common.scheduling.UpdateProgress.SUCCESS;
import static org.atlasapi.persistence.lookup.entry.EquivRefs.Direction.BIDIRECTIONAL;

public class LookupRefUpdateTask extends ScheduledTask {

    private static final String ID_FIELD = "aid";

    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final MongoCollection<DBObject> lookupCollection;
    private final LookupEntryTranslator entryTranslator;
    private final MongoLookupEntryStore entryStore;
    private final DBCollection progressCollection;
    private final String scheduleKey;


    private static final Set<Long> seen = Sets.newHashSet();

    private final Predicate<Object> notNull = Predicates.not(Predicates.isNull());
    
    public LookupRefUpdateTask(
            DatabasedMongoClient mongo,
            String lookupCollectionName,
            DBCollection progressCollection
    ) {
        this.lookupCollection = mongo.collection(lookupCollectionName, DBObject.class);
        this.entryTranslator = new LookupEntryTranslator();
        this.entryStore = new MongoLookupEntryStore(
                mongo,
                lookupCollectionName,
                new NoLoggingPersistenceAuditLog(),
                ReadPreference.primary());
        this.progressCollection = checkNotNull(progressCollection);
        this.scheduleKey = "lookuprefupdate";
    }

    @Override
    protected void runTask() {
        Long start = getStart();
        log.info("Started: {} from {}", scheduleKey, startProgress(start));

        Iterator<DBObject> aids = lookupCollection.find(aidGreaterThan(start))
                .projection(selectAidOnly())
                .sort(new Document(ID_FIELD, 1))
                .batchSize(1000)
                .noCursorTimeout(true)
                .iterator();
        
        UpdateProgress processed = UpdateProgress.START;

        Long aid = null;
        try {
            while (aids.hasNext() && shouldContinue()) {
                try {
                    aid = TranslatorUtils.toLong(aids.next(), ID_FIELD);
                    updateRefIds(aid);
                    reportStatus(String.format("%s. Processing %s", processed, aid));
                    processed = processed.reduce(SUCCESS);
                    if (processed.getTotalProgress() % 100 == 0) {
                        updateProgress(aid);
                    }
                } catch (Exception e) {
                    processed = processed.reduce(FAILURE);
                    log.error("ChildRef update failed: " + aid, e);
                }
            }
        } catch (Exception e) {
            log.error("Exception running task " + scheduleKey, e);
            persistProgress(false, aid);
            throw Throwables.propagate(e);
        }
        reportStatus(processed.toString());
        persistProgress(shouldContinue(), aid);
    }

    private long getStart() {
        DBObject doc = Objects.firstNonNull(progressCollection.findOne(scheduleKey), new BasicDBObject());
        return Objects.firstNonNull(TranslatorUtils.toLong(doc, ID_FIELD),0L);
    }

    private void updateRefIds(Long aid) {
        if (seen.contains(aid)) {
            return;
        }
        LookupEntry entry = entryTranslator.fromDbo(lookupCollection.find(new Document(ID_FIELD, aid)).first());
        if (allRefsHaveIds(entry)) {
            return;
        }
        if (entry.equivalents().size() == 1
                && entry.directEquivalents().getLookupRefs().size() == 1
                && entry.explicitEquivalents().getLookupRefs().size() == 1
                && entry.blacklistedEquivalents().getLookupRefs().size() == 0
        ) {
            updateSolo(entry);
        } else {
            updateEntryWithEquivalents(entry);
        }
    }

    private boolean allRefsHaveIds(LookupEntry entry) {
        Iterable<LookupRef> refs = Iterables.concat(
                entry.equivalents(),
                entry.directEquivalents().getLookupRefs(),
                entry.explicitEquivalents().getLookupRefs(),
                entry.blacklistedEquivalents().getLookupRefs()
        );
        return Iterables.all(Iterables.transform(refs, LookupRef.TO_ID), notNull);
    }

    private void updateEntryWithEquivalents(LookupEntry entry) {
        Set<LookupEntry> equivalentEntries = ImmutableSet.copyOf(entryStore.entriesForCanonicalUris(Iterables.transform(entry.equivalents(), LookupRef.TO_URI)));
        ImmutableMap<String, LookupEntry> entryIndex = Maps.uniqueIndex(equivalentEntries, LookupEntry.TO_ID);
        List<LookupEntry> updated = Lists.newArrayListWithCapacity(equivalentEntries.size());
        for (LookupEntry lookupEntry : equivalentEntries) {
            updated.add(updateRefs(lookupEntry, entryIndex));
        }
        for (LookupEntry updatedEntry : updated) {
            entryStore.store(updatedEntry);
        }
        Iterables.addAll(seen, Iterables.transform(equivalentEntries, Functions.compose(LookupRef.TO_ID, LookupEntry.TO_SELF)));
    }

    private LookupEntry updateRefs(LookupEntry e, ImmutableMap<String, LookupEntry> entryIndex) {
        EquivRefs direct = fillInMissingIds(e.directEquivalents(), entryIndex);
        EquivRefs explicit = fillInMissingIds(e.explicitEquivalents(), entryIndex);
        EquivRefs blacklisted = fillInMissingIds(e.blacklistedEquivalents(), entryIndex);
        Set<LookupRef> equivs = fillInMissingIds(e.equivalents(), entryIndex);
        return new LookupEntry(
                e.uri(),
                e.id(),
                e.lookupRef(),
                e.aliasUrls(),
                e.aliases(),
                direct,
                explicit,
                blacklisted,
                equivs,
                e.created(),
                e.updated(),
                e.transitivesUpdated(),
                e.activelyPublished()
        );
    }

    private Set<LookupRef> fillInMissingIds(Set<LookupRef> refs, ImmutableMap<String, LookupEntry> entryIndex) {
        Set<LookupRef> updated = Sets.newHashSet();
        for (LookupRef ref : refs) {
            if (ref.id() == null) {
                ref = new LookupRef(ref.uri(), entryIndex.get(ref.uri()).id(), ref.publisher(), ref.category());
            }
            updated.add(ref);
        }
        return updated;
    }

    private EquivRefs fillInMissingIds(EquivRefs refs, ImmutableMap<String, LookupEntry> entryIndex) {
        ImmutableMap.Builder<LookupRef, EquivRefs.Direction> updated = ImmutableMap.builder();
        for (Map.Entry<LookupRef, EquivRefs.Direction> entry : refs.getEquivRefsAsMap().entrySet()) {
            LookupRef ref = entry.getKey();
            if (ref.id() == null) {
                ref = new LookupRef(ref.uri(), entryIndex.get(ref.uri()).id(), ref.publisher(), ref.category());
            }
            updated.put(ref, entry.getValue());
        }
        return EquivRefs.of(updated.build());
    }

    private void updateSolo(LookupEntry e) {
        LookupRef ref = e.lookupRef();
        if (ref.id() == null) {
            ref = new LookupRef(e.uri(), e.id(), e.lookupRef().publisher(), e.lookupRef().category());
        }
        ImmutableSet<LookupRef> refs = ImmutableSet.of(ref);
        EquivRefs equivRefs = EquivRefs.of(ref, BIDIRECTIONAL);
        lookupCollection.replaceOne(
                new Document("_id", e.uri()),
                entryTranslator.toDbo(
                        new LookupEntry(
                                e.uri(),
                                e.id(),
                                ref,
                                e.aliasUrls(),
                                e.aliases(),
                                equivRefs,
                                equivRefs,
                                EquivRefs.of(),
                                refs,
                                e.created(),
                                e.updated(),
                                e.transitivesUpdated(),
                                e.activelyPublished()
                        )
                ),
                new ReplaceOptions().upsert(true)
        );
    }

    private Document selectAidOnly() {
        return new Document(ImmutableMap.of(MongoConstants.ID,0,ID_FIELD,1));
    }

    private Document aidGreaterThan(Long start) {
        return new Document(ID_FIELD, new Document(MongoConstants.GREATER_THAN, start));
    }
    
    
    public void updateProgress(Long aid) {
        progressCollection.save(new BasicDBObject(ImmutableMap.of(
            MongoConstants.ID, scheduleKey, ID_FIELD, aid
        )));
    }
    
    private void persistProgress(boolean finished, Long aid) {
        if (finished) {
            updateProgress(0L);
            log.info("Finished: {}", scheduleKey);
        } else {
            if (aid != null) {
                updateProgress(aid);
                log.info("Stopped: {} at {}", scheduleKey, aid);
            }
        }
    }

    private String startProgress(long progress) {
        if (progress == 0) {
            return "start";
        }
        return String.format("%s", progress);
    }
    

}
