package org.atlasapi.remotesite.pa.deletes;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Queues;
import com.google.common.collect.SetMultimap;
import com.metabroadcast.common.scheduling.StatusReporter;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.atlasapi.equiv.update.tasks.ScheduleTaskProgressStore;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

public class PaContentDeactivator {

    private static final Logger LOG = LoggerFactory.getLogger(PaContentDeactivator.class);
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static final Pattern SERIES_ID_PATTERN = Pattern.compile("<series_id>([0-9]*)</series_id>");
    private static final Pattern PROGRAMME_ID_PATTERN = Pattern.compile("<prog_id>([0-9]*)</prog_id>");

    public static final String PA_SERIES_NAMESPACE = "pa:brand";
    public static final String PA_PROGRAMME_NAMESPACE = "pa:episode";

    private static final String CONTAINER = "container";
    public static final Pattern FILENAME_DATE_EXTRACTOR = Pattern.compile("^([0-9]{4})([0-9]{2})([0-9]{2})_archiveID.xml$");

    private final ContentLister contentLister;
    private final ContentWriter contentWriter;
    private final DBCollection childrenDb;
    private final ScheduleTaskProgressStore progressStore;
    private final ThreadPoolExecutor threadPool;

    private AtomicInteger deactivated = new AtomicInteger(0);
    private AtomicInteger processed = new AtomicInteger(0);
    private AtomicInteger reactivated = new AtomicInteger(0);

    public PaContentDeactivator(ContentLister contentLister, ContentWriter contentWriter,
                                ScheduleTaskProgressStore progressStore, DBCollection childrenDb) {
        this.contentLister = checkNotNull(contentLister);
        this.contentWriter = checkNotNull(contentWriter);
        this.progressStore = checkNotNull(progressStore);
        this.threadPool = createThreadPool(20);
        this.childrenDb = checkNotNull(childrenDb);
    }

    public void deactivate(File file, Boolean dryRun, StatusReporter reporter) throws IOException {
        List<String> lines = Files.readAllLines(file.toPath(), UTF8);
        reporter.reportStatus(
                String.format(
                        "Loading active IDs from file %s",
                        file.getAbsolutePath()
                )
        );
        ImmutableSetMultimap<String, String> typeToIds = extractAliases(lines);
        reporter.reportStatus(
                String.format(
                        "Loaded %d active IDs from file %s",
                        typeToIds.entries().size(),
                        file.getAbsolutePath()
                )
        );
        deactivated = new AtomicInteger(0);
        processed = new AtomicInteger(0);
        reactivated = new AtomicInteger(0);
        DateTime ignoreIfModifiedAfter = extractCutoffTimeFromFilename(file.getName()).minusDays(1);
        LOG.info(
                String.format(
                        "Not deactivating content ingest/updated after %s",
                        ignoreIfModifiedAfter.toString()
                )
        );
        deactivate(typeToIds, dryRun, reporter, ignoreIfModifiedAfter);
    }

    public void deactivate(
            Multimap<String, String> paNamespaceToAliases,
            Boolean dryRun,
            StatusReporter reporter,
            DateTime ignoreIfModifiedAfter
    ) throws IOException {
        Predicate<Content> shouldDeactivatePredicate = new PaContentDeactivationPredicate(
                ignoreIfModifiedAfter, paNamespaceToAliases
        );
        deactivateChildren(dryRun, shouldDeactivatePredicate, reporter);
        deactivateContainers(dryRun, shouldDeactivatePredicate, reporter);
    }


    private void deactivateContainers(
            Boolean dryRun,
            Predicate<Content> shouldDeactivatePredicate,
            StatusReporter reporter
    ) {
        String containerTaskName = getClass().getSimpleName() + "containers";

        final Iterator<Content> containerItr = contentLister.listContent(
                createListingCriteria(
                        ImmutableList.of(ContentCategory.CONTAINER),
                        Optional.fromNullable(progressStore.progressForTask(containerTaskName))
                )
        );

        deactivateContent(containerItr, shouldDeactivatePredicate, dryRun, containerTaskName, reporter);
    }

    private void deactivateChildren(
            Boolean dryRun,
            Predicate<Content> shouldDeactivatePredicate,
            StatusReporter reporter
    ) {
        String childrenTaskName = getClass().getSimpleName() + "children";
        ImmutableList<ContentCategory> childCategories = ImmutableList.of(
                ContentCategory.CHILD_ITEM,
                ContentCategory.TOP_LEVEL_ITEM
        );
        final Iterator<Content> childItr = contentLister.listContent(
                createListingCriteria(
                        childCategories,
                        Optional.fromNullable(progressStore.progressForTask(childrenTaskName))
                )
        );
        deactivateContent(childItr, shouldDeactivatePredicate, dryRun, childrenTaskName, reporter);
    }

    private void deactivateContent(
            Iterator<Content> itr,
            Predicate<Content> shouldDeactivatePredicate,
            Boolean dryRun,
            String taskName,
            StatusReporter reporter
    ) {
        while (itr.hasNext()) {
            Content content = itr.next();
            int i = processed.incrementAndGet();
            LOG.debug("Processing item #{} id: {}", i, content.getId());
            if (shouldDeactivatePredicate.apply(content) && hasNoGenericChildren(content)) {
                if (!dryRun) {
                    content.setActivelyPublished(false);
                    threadPool.submit(contentWritingRunnable(content));
                }
                LOG.debug("Deactivating item #{} id: {}", deactivated.incrementAndGet(), content.getId());
            } else if (!content.isActivelyPublished()) {
                if (!dryRun) {
                    content.setActivelyPublished(true);
                    threadPool.submit(contentWritingRunnable(content));
                }
                LOG.debug("Reactivating item #{} id: {}", reactivated.incrementAndGet(), content.getId());
            }
            if (i % 1000 == 0) {
                progressStore.storeProgress(
                        taskName,
                        ContentListingProgress.progressFrom(content)
                );
                LOG.debug("Processed {} items, saving progress", i);
            }
            reporter.reportStatus(String.format("Processed %d Deactivated %d", i, deactivated.get()));
        }
        LOG.debug("Deactivated {} pieces of content", deactivated.get());
        LOG.debug("Reactivated {} pieces of content", reactivated.get());
        reporter.reportStatus(
                String.format(
                        "Finished processing %d items, deactivated %d",
                        processed.get(),
                        deactivated.get()
                )
        );
        progressStore.storeProgress(taskName, ContentListingProgress.START);
    }

    private Runnable contentWritingRunnable(final Content content) {
        return new Runnable() {
            @Override
            public void run() {
                if (content instanceof Container) {
                    contentWriter.createOrUpdate((Container) content);
                }
                if (content instanceof Item) {
                    contentWriter.createOrUpdate((Item) content);
                }
            }
        };
    }

    /*
        Generically described children are not returned by getChildRefs,
        thus we must check the DB for any children with this content as its container.
     */
    private boolean hasNoGenericChildren(Content content) {
        if (!(content instanceof Container)) {
            return true;
        }
        DBObject dbQuery = where().fieldEquals(CONTAINER, content.getCanonicalUri()).build();
        return childrenDb.find(dbQuery).count() < 1;
    }


    private ThreadPoolExecutor createThreadPool(Integer maxThreads) {
        return new ThreadPoolExecutor(
                maxThreads,
                maxThreads,
                500,
                TimeUnit.MILLISECONDS,
                Queues.<Runnable>newLinkedBlockingQueue(maxThreads),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    private ContentListingCriteria createListingCriteria(
            Iterable<ContentCategory> categories,
            Optional<ContentListingProgress> progress
    ) {
        ContentListingCriteria.Builder criteria = ContentListingCriteria.defaultCriteria()
                .forContent(categories)
                .forPublisher(Publisher.PA);
        if (progress.isPresent()) {
            return criteria.startingAt(progress.get()).build();
        }
        return criteria.build();
    }

    /*
        Expects lines to be from an XML file provided by PA, containing their IDs by content type.
        This translates into a map of PA alias namespaces to values.

        Series aren't handled here as we lack a reliable mapping from PA's IDs
        to their URIs in atlas. They are handled by removing empty Series elsewhere
    */
    private ImmutableSetMultimap<String, String> extractAliases(List<String> lines) throws IOException {
        SetMultimap<String, String> typeToIds = MultimapBuilder.hashKeys().hashSetValues().build();
        for (String line : lines) {
            Matcher programmeMatcher = PROGRAMME_ID_PATTERN.matcher(line);
            if (programmeMatcher.find()) {
                LOG.debug("Matched {} as programme ID", programmeMatcher.group(1));
                typeToIds.put(PA_PROGRAMME_NAMESPACE, programmeMatcher.group(1));
                continue;
            }
            Matcher seriesMatcher = SERIES_ID_PATTERN.matcher(line);
            if (seriesMatcher.find()) {
                LOG.debug("Matched {} as series ID", seriesMatcher.group(1));
                typeToIds.put(PA_SERIES_NAMESPACE, seriesMatcher.group(1));
                continue;
            }
            LOG.warn("Line: {} matched no regex for ID extraction, skipping...", line);
        }
        return ImmutableSetMultimap.copyOf(typeToIds);
    }

    /*
        Extracts the date from the file name of a PA ID archive file.
        Content ingested or updated after (this date - 1d) will not be candidate for deactivation
    */
    @VisibleForTesting
    DateTime extractCutoffTimeFromFilename(String name) {
        Matcher matcher = FILENAME_DATE_EXTRACTOR.matcher(name);
        checkArgument(matcher.matches(), "Unable to extract cut off date from filename: " + name);
        return new DateTime(
                Integer.parseInt(matcher.group(1)),
                Integer.parseInt(matcher.group(2)),
                Integer.parseInt(matcher.group(3)),
                0,
                0
        );
    }
}