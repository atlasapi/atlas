package org.atlasapi.equiv;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.scheduling.UpdateProgress.FAILURE;
import static com.metabroadcast.common.scheduling.UpdateProgress.SUCCESS;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.mongo.LookupEntryTranslator;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;

public class LookupRefUpdateTask extends ScheduledTask {

    private static final String ID_FIELD = "aid";

    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final DBCollection lookupCollection;
    private final LookupEntryTranslator entryTranslator;
    private final MongoLookupEntryStore entryStore;
    private final DBCollection progressCollection;
    private final String scheduleKey;


    private static final Set<Long> seen = Sets.newHashSet();

    private final Predicate<Object> notNull = Predicates.not(Predicates.isNull());
    
    public LookupRefUpdateTask(DBCollection lookupCollection, DBCollection progressCollection) {
        this.lookupCollection = checkNotNull(lookupCollection);
        this.entryTranslator = new LookupEntryTranslator();
        this.entryStore = new MongoLookupEntryStore(lookupCollection,
                new NoLoggingPersistenceAuditLog(),
                ReadPreference.primary());
        this.progressCollection = checkNotNull(progressCollection);
        this.scheduleKey = "lookuprefupdate";
    }

    @Override
    protected void runTask() {
        Long start = getStart();
        log.info("Started: {} from {}", scheduleKey, startProgress(start));
        
        DBCursor aids = lookupCollection.find(aidGreaterThan(start),
            selectAidOnly())
            .sort(new BasicDBObject(ID_FIELD,1))
            .batchSize(1000)
            .addOption(Bytes.QUERYOPTION_NOTIMEOUT);
        
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
        LookupEntry entry = entryTranslator.fromDbo(lookupCollection.findOne(new BasicDBObject(ID_FIELD, aid)));
        if (allRefsHaveIds(entry)) {
            return;
        }
        if (entry.equivalents().size() == 1) {
            updateSolo(entry);
        } else {
            updateEntryWithEquivalents(entry);
        }
    }

    private boolean allRefsHaveIds(LookupEntry entry) {
        Iterable<LookupRef> refs = Iterables.concat(entry.equivalents(), entry.directEquivalents(), entry.explicitEquivalents());
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

    private LookupEntry updateRefs(LookupEntry e,
            ImmutableMap<String, LookupEntry> entryIndex) {
        Set<LookupRef> direct = updateIds(e.directEquivalents(), entryIndex);
        Set<LookupRef> explicit = updateIds(e.explicitEquivalents(), entryIndex);
        Set<LookupRef> equivs = updateIds(e.equivalents(), entryIndex);
        return new LookupEntry(e.uri(),e.id(),e.lookupRef(),e.aliasUrls(),e.aliases(),direct,explicit,equivs,e.created(),e.updated(),e.activelyPublished());
    }

    private Set<LookupRef> updateIds(Set<LookupRef> refs,
            ImmutableMap<String, LookupEntry> entryIndex) {
        Set<LookupRef> updated = Sets.newHashSet();
        for (LookupRef ref : refs) {
            if (ref.id() == null) {
                ref = new LookupRef(ref.uri(), entryIndex.get(ref.uri()).id(), ref.publisher(), ref.category());
            }
            updated.add(ref);
        }
        return updated;
    }

    private void updateSolo(LookupEntry e) {
        LookupRef ref = e.lookupRef();
        if (ref.id() == null) {
            ref = new LookupRef(e.uri(), e.id(), e.lookupRef().publisher(), e.lookupRef().category());
        }
        ImmutableSet<LookupRef> refs = ImmutableSet.of(ref);
        lookupCollection.save(entryTranslator.toDbo(
            new LookupEntry(e.uri(), e.id(), ref, e.aliasUrls(), e.aliases(), refs, refs, refs, e.created(), e.updated())
        ));
    }

    private BasicDBObject selectAidOnly() {
        return new BasicDBObject(ImmutableMap.of(MongoConstants.ID,0,ID_FIELD,1));
    }

    private BasicDBObject aidGreaterThan(Long start) {
        return new BasicDBObject(ID_FIELD, new BasicDBObject(MongoConstants.GREATER_THAN, start));
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
