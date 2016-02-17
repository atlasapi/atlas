package org.atlasapi.system;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Queues;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.content.listing.ProgressStore;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

public class PaContentDeactivator {

    private static final Logger LOG = LoggerFactory.getLogger(PaContentDeactivator.class);
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static final Pattern SERIES_ID_PATTERN = Pattern.compile("<series_id>([0-9]*)</series_id>");
    private static final Pattern SEASON_ID_PATTERN = Pattern.compile("<season_id>([0-9]*)</season_id>");
    private static final Pattern PROGRAMME_ID_PATTERN = Pattern.compile("<prog_id>([0-9]*)</prog_id>");

    private static final String PA_SERIES_NAMESPACE = "pa:brand";
    private static final String PA_SEASON_NAMESPACE = "pa:series";
    private static final String PA_PROGRAMME_NAMESPACE = "pa:episode";

    private static final Function<LookupEntry, Long> LOOKUP_ENTRY_TO_ID = new Function<LookupEntry, Long>() {
        @Override
        public Long apply(LookupEntry lookupEntry) {
            return lookupEntry.id();
        }
    };

    private final LookupEntryStore lookupStore;
    private final ContentLister contentLister;
    private final ContentWriter contentWriter;
    private final ProgressStore progressStore;
    private final ThreadPoolExecutor threadPool;

    public PaContentDeactivator(LookupEntryStore lookupStore, ContentLister contentLister,
                                ContentWriter contentWriter, ProgressStore progressStore) {
        this.lookupStore = checkNotNull(lookupStore);
        this.contentLister = checkNotNull(contentLister);
        this.contentWriter = checkNotNull(contentWriter);
        this.progressStore = checkNotNull(progressStore);
        this.threadPool = createThreadPool(20);
    }

    public void deactivate(File file, Boolean dryRun) throws IOException {
        List<String> lines = Files.readAllLines(file.toPath(), UTF8);
        ImmutableSetMultimap<String, String> typeToIds = extractAliases(lines);
        deactivate(typeToIds, dryRun);
    }

    public void deactivate(Multimap<String, String> paNamespaceToAliases, Boolean dryRun) throws IOException {
        final ImmutableSet<Long> activeIds = ImmutableSet.copyOf(
                Sets.newTreeSet(resolvePaAliasesToIds(paNamespaceToAliases))
        );

        Predicate<Content> shouldDeactivatePredicate = new PaContentDeactivationPredicate(activeIds);

        String childrenTaskName = getClass().getSimpleName() + "children";
        ImmutableList<ContentCategory> childCategories = ImmutableList.of(
                ContentCategory.CHILD_ITEM,
                ContentCategory.TOP_LEVEL_ITEM
        );
        final Iterator<Content> childItr = contentLister.listContent(
                createListingCriteria(childCategories, progressStore.progressForTask(childrenTaskName))
        );
        deactivateContent(childItr, shouldDeactivatePredicate, dryRun, childrenTaskName);

        String containerTaskName = getClass().getSimpleName() + "containers";
        final Iterator<Content> containerItr = contentLister.listContent(
                createListingCriteria(
                        ImmutableList.of(ContentCategory.CONTAINER),
                        progressStore.progressForTask(containerTaskName)
                )
        );
        deactivateContent(containerItr, shouldDeactivatePredicate, dryRun, containerTaskName);
    }

    private void deactivateContent(
            Iterator<Content> itr,
            Predicate<Content> shouldDeactivatePredicate,
            Boolean dryRun,
            String taskName
    ) {
        AtomicInteger deactivated = new AtomicInteger(0);
        AtomicInteger processed = new AtomicInteger(0);
        while (itr.hasNext()) {
            Content content = itr.next();
            int i = processed.incrementAndGet();
            LOG.debug("Processing item #{} id: {}", i, content.getId());
            if (shouldDeactivatePredicate.apply(content)) {
                if (!dryRun) {
                    threadPool.submit(contentDeactivatingRunnable(content));
                }
                LOG.debug("Deactivating item #{} id: {}", deactivated.incrementAndGet(), content.getId());
            }
            if (i % 1000 == 0) {
                progressStore.storeProgress(
                        taskName,
                        ContentListingProgress.progressFrom(content)
                );
                LOG.debug("Processed {} items, saving progress", i);
            }
        }
        LOG.debug("Deactivated {} pieces of content", deactivated.get());
        progressStore.storeProgress(taskName, ContentListingProgress.START);
    }

    private Runnable contentDeactivatingRunnable(final Content content) {
        return new Runnable() {
            @Override
            public void run() {
                content.setActivelyPublished(false);
                if (content instanceof Container) {
                    contentWriter.createOrUpdate((Container) content);
                }
                if (content instanceof Item) {
                    contentWriter.createOrUpdate((Item) content);
                }
            }
        };
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
            Matcher seasonMatcher = SEASON_ID_PATTERN.matcher(line);
            if (seasonMatcher.find()) {
                LOG.debug("Matched {} as season ID", seasonMatcher.group(1));
                typeToIds.put(PA_SEASON_NAMESPACE, seasonMatcher.group(1));
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

    private ImmutableSet<Long> resolvePaAliasesToIds(Multimap<String, String> typeToIds) {
        ImmutableSet.Builder<Long> ids = ImmutableSet.builder();
        for (Map.Entry<String, Collection<String>> entry : typeToIds.asMap().entrySet()) {
            for (List<String> idPartition : Iterables.partition(entry.getValue(), 200)) {
                ids.addAll(lookupIdForPaAlias(entry.getKey(), idPartition));
            }
        }
        return ids.build();
    }

    private ImmutableSet<Long> lookupIdForPaAlias(String namespace, Iterable<String> value) {
        Iterable<LookupEntry> entriesForId = lookupStore.entriesForAliases(
                Optional.of(namespace), value
        );
        return ImmutableSet.copyOf(Iterables.transform(entriesForId, LOOKUP_ENTRY_TO_ID));
    }
}