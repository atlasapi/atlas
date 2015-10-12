package org.atlasapi.equiv;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.update;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.scheduling.UpdateProgress.FAILURE;
import static com.metabroadcast.common.scheduling.UpdateProgress.SUCCESS;
import static org.atlasapi.persistence.content.ContentCategory.CONTAINER;
import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;
import static org.atlasapi.persistence.content.listing.ContentListingProgress.progressFrom;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.atlasapi.equiv.update.tasks.ScheduleTaskProgressStore;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.SeriesRef;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.content.mongo.MongoContentTables;
import org.atlasapi.persistence.media.entity.ChildRefTranslator;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;
import org.atlasapi.persistence.media.entity.SeriesRefTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoBuilders;
import com.metabroadcast.common.persistence.mongo.MongoUpdateBuilder;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.mongodb.BasicDBList;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class ChildRefUpdateTask extends ScheduledTask {
    
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ContentLister contentStore;
    private final ContentResolver resolver;
    private final ScheduleTaskProgressStore progressStore;

    private final DBCollection containers;
    private final DBCollection programmeGroups;
    private final DBCollection children;

    private ImmutableList<Publisher> publishers;
    private String scheduleKey;
    private final ChildRefTranslator childRefTranslator = new ChildRefTranslator();
    private final SeriesRefTranslator seriesRefTranslator = new SeriesRefTranslator();

    public ChildRefUpdateTask(ContentLister lister, ContentResolver resolver, DatabasedMongo mongo, ScheduleTaskProgressStore progressStore) {
        this.contentStore = lister;
        this.resolver = resolver;
        
        MongoContentTables contentTables = new MongoContentTables(mongo);
        containers = contentTables.collectionFor(ContentCategory.CONTAINER);
        programmeGroups = contentTables.collectionFor(ContentCategory.PROGRAMME_GROUP);
        children = contentTables.collectionFor(ContentCategory.CHILD_ITEM);
        
        this.progressStore = progressStore;
    }
    
    public ChildRefUpdateTask forPublishers(Publisher... publishers) {
        this.publishers = ImmutableList.copyOf(publishers);
        this.scheduleKey = "childref" + Joiner.on("-").join(this.publishers);
        return this;
    }

    @Override
    protected void runTask() {
        
        ContentListingProgress progress = progressStore.progressForTask(scheduleKey);
        log.info("Started: {} from {}", scheduleKey, startProgress(progress));
        
        Iterator<Content> children = contentStore.listContent(defaultCriteria().forPublishers(publishers).forContent(ImmutableList.of(CONTAINER)).startingAt(progress).build());

        UpdateProgress processed = UpdateProgress.START;
        Content content = null;

        try {
            while (children.hasNext() && shouldContinue()) {
                try {
                    content = children.next();
                    updateContainerReferences((Container) content);
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

    public void updateContainerReferences(Container container) {
        Preconditions.checkArgument(container.getId() != null, "null id");
        udpateContainerReferences(container);
    }

    /*
     * Update references for container and its descendant content.
     * 
     * 1. resolve series and create uri -> child ref map.
     * 
     * 2. resolve child items into series uri -> item child ref multimap. if the item
     * has no series (because its an item or episode without series) the key
     * "none" is used.
     * 
     * 3. update all the children in the multimap from 2. using container and map from 1
     * to set container id and series id (if present). 
     * 
     * 4. update each series if there are any, setting container id and it's child
     * refs.
     * 
     * 5. update the container, set the series refs (if present) and child refs.
     */
    private void udpateContainerReferences(Container container) {
        
        Map<String, SeriesRef> seriesRefs = getSeriesRefs(container);
        
        Multimap<String, ChildRef> childRefs = getChildRefs(container);
        
        updateChildrensRefs(childRefs, container, seriesRefs);
        
        updateSeriesRefs(container, seriesRefs, childRefs);
        
        updateContainersRefs(container, seriesRefs, childRefs);
        
    }

    private void updateChildrensRefs(Multimap<String, ChildRef> childRefs, Container container,
                                     Map<String, SeriesRef> seriesRefs) {
        for (Entry<String, ChildRef> childRef : childRefs.entries()) {
            children.update(
                where().idEquals(childRef.getValue().getUri()).build(),
                updateForItem(container, seriesRefs.get(childRef.getKey()))
            );
        }
    }

    private Multimap<String, ChildRef> getChildRefs(Container container) {
        Multimap<String, ChildRef> seriesChildRefs = LinkedListMultimap.create();
        
        ResolvedContent resolvedEpisodes = resolver.findByCanonicalUris(Iterables.transform(container.getChildRefs(),ChildRef.TO_URI));
        for (Item item : Iterables.filter(resolvedEpisodes.getAllResolvedResults(), Item.class)) {
            if (!item.isActivelyPublished()) {
                continue;
            }
            
            ChildRef childRef = item.childRef();
            if (item instanceof Episode && ((Episode)item).getSeriesRef() != null) {
                    String seriesUri = ((Episode)item).getSeriesRef().getUri();
                    seriesChildRefs.put(seriesUri, childRef);
            } else {
                seriesChildRefs.put("none", childRef);
            }
        }
        return seriesChildRefs;
    }

    private Map<String, SeriesRef> getSeriesRefs(Container container) {
        Map<String, SeriesRef> seriesRefs = Maps.newLinkedHashMap();

        if (container instanceof Brand) {
            ResolvedContent resolvedSeries = resolver.findByCanonicalUris(Iterables.transform(((Brand)container).getSeriesRefs(),SeriesRef.TO_URI));
            for (Series series : Iterables.filter(resolvedSeries.getAllResolvedResults(), Series.class)) {
                seriesRefs.put(series.getCanonicalUri(), series.seriesRef());
            }
        } else if (container instanceof Series) {
            // if this is a top level series then all its children *series* ref is to this too.
            seriesRefs.put(container.getCanonicalUri(), ((Series)container).seriesRef());
        } else {
            throw new IllegalArgumentException("Unexpected Container type: " + container.getClass().getSimpleName());
        }
        return seriesRefs;
    }

    private void updateSeriesRefs(Container container, Map<String, SeriesRef> seriesRefs,
                                  Multimap<String, ChildRef> seriesChildRefs) {
        if (seriesRefs.size() == 1 && seriesRefs.containsKey(container.getCanonicalUri())) {
            return; //container is a top level series.
        }
        for (String seriesUri : Iterables.transform(seriesRefs.values(), SeriesRef.TO_URI)) {
            MongoUpdateBuilder seriesUpdate = update()
                .setField(ContainerTranslator.CONTAINER_ID, container.getId());
            Collection<ChildRef> seriesChildren = seriesChildRefs.get(seriesUri);
            if (!seriesChildren.isEmpty()) {
                seriesUpdate.setField(ContainerTranslator.CHILDREN_KEY, dbosFromChildRefs(seriesChildren));
            }
            programmeGroups.update(where().idEquals(seriesUri).build(), seriesUpdate.build());
        }
    }

    private void updateContainersRefs(Container container, Map<String, SeriesRef> seriesRefs,
                                     Multimap<String, ChildRef> childRefs) {
        MongoUpdateBuilder brandUpdate = update()
            .setField(ContainerTranslator.CHILDREN_KEY, dbosFromChildRefs(childRefs.values()));
        if (!seriesRefs.isEmpty()) {
            brandUpdate.setField(ContainerTranslator.FULL_SERIES_KEY, dbosFromSeriesRefs(seriesRefs.values()));
        }
        containers.update(where().idEquals(container.getCanonicalUri()).build(), brandUpdate.build());
    }

    private DBObject updateForItem(Container container, SeriesRef seriesRef) {
        MongoUpdateBuilder childUpdate = MongoBuilders.update().setField(ItemTranslator.CONTAINER_ID, container.getId());
        if (seriesRef != null) {
            childUpdate.setField(ItemTranslator.SERIES_ID, seriesRef.getId());
        }
        DBObject update = childUpdate.build();
        return update;
    }

    private BasicDBList dbosFromChildRefs(Iterable<ChildRef> childRefs) {
        return childRefTranslator.toDBList(ChildRef.dedupeAndSort(childRefs));
    }
    
    private BasicDBList dbosFromSeriesRefs(Iterable<SeriesRef> childRefs) {
        return seriesRefTranslator.toDBList(SeriesRef.dedupeAndSort(childRefs));
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
