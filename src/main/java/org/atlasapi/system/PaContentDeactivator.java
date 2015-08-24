package org.atlasapi.system;

import com.google.api.client.repackaged.com.google.common.base.Strings;
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
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
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
import java.util.Set;
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

    private static final Funnel<Long> LONG_FUNNEL = new Funnel<Long>() {
        @Override
        public void funnel(Long aLong, PrimitiveSink sink) {
            sink.putLong(aLong);
        }
    };
    private static final Function<LookupEntry, Long> LOOKUP_ENTRY_TO_ID = new Function<LookupEntry, Long>() {
        @Override
        public Long apply(LookupEntry lookupEntry) {
            return lookupEntry.id();
        }
    };
    private static final Predicate<String> IS_FILM = new Predicate<String>() {
        @Override
        public boolean apply(String s) {
            return !Strings.isNullOrEmpty(s) && s.startsWith("http://pressassociation.com/films/");
        }
    };

    private final LookupEntryStore lookupStore;
    private final ContentLister contentLister;
    private final ContentWriter contentWriter;
    private final ProgressStore progressStore;

    public PaContentDeactivator(LookupEntryStore lookupStore, ContentLister contentLister,
            ContentWriter contentWriter, ProgressStore progressStore) {
        this.lookupStore = checkNotNull(lookupStore);
        this.contentLister = checkNotNull(contentLister);
        this.contentWriter = checkNotNull(contentWriter);
        this.progressStore = checkNotNull(progressStore);
    }

    public void deactivate(File file, Integer threads) throws IOException {
        List<String> lines = Files.readAllLines(file.toPath(), UTF8);
        ImmutableSetMultimap<String, String> typeToIds = extractAliases(lines);
        deactivate(typeToIds, threads);
    }

    public void deactivate(Multimap<String, String> paNamespaceToAliases, Integer threads) {
        ImmutableSet<Long> activeAtlasContentIds = resolvePaAliasesToIds(paNamespaceToAliases);
        final BloomFilter<Long> filter = bloomFilterFor(activeAtlasContentIds);
        final Iterator<Content> contentIterator = contentLister.listContent(
                createListingCriteria(progressStore.progressForTask(getClass().getSimpleName()))
        );
        final ThreadPoolExecutor executor = createThreadPool(threads);
        final AtomicInteger progressCount = new AtomicInteger();
        while (contentIterator.hasNext()) {
            Content content = contentIterator.next();
            if (!Iterables.any(content.getAllUris(), IS_FILM) && !filter.mightContain(content.getId()) &&
                    !content.getGenericDescription()) {
                LOG.info("Content {} - {} not in bloom filter, deactivating...",
                        content.getClass().getSimpleName(), content.getId());
                executor.submit(contentDeactivatingRunnable(content, progressCount));
            }
        }
        LOG.info("Deactivated {} items", progressCount.get());
    }

    private Runnable contentDeactivatingRunnable(final Content content, final AtomicInteger progressCount) {
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
                int count = progressCount.incrementAndGet();
                if (count % 1000 == 0) {
                    progressStore.storeProgress(
                            getClass().getSimpleName(),
                            ContentListingProgress.progressFrom(content, count)
                    );
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

    private ContentListingCriteria createListingCriteria(Optional<ContentListingProgress> progress) {
        ImmutableList<ContentCategory> categories = ImmutableList.of(
                ContentCategory.CONTAINER,
                ContentCategory.CHILD_ITEM,
                ContentCategory.TOP_LEVEL_ITEM
        );
        ContentListingCriteria.Builder criteria = ContentListingCriteria.defaultCriteria()
                .forContent(categories)
                .forPublisher(Publisher.PA);
        if (progress.isPresent()) {
            return criteria.startingAt(progress.get()).build();
        }
        return criteria.build();
    }

    private BloomFilter<Long> bloomFilterFor(Set<Long> ids) {
        BloomFilter<Long> filter = BloomFilter.create(LONG_FUNNEL, ids.size());
        return populateBloomFilter(filter, ids);
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

    private BloomFilter<Long> populateBloomFilter(BloomFilter<Long> filter, Iterable<Long> longs) {
        for (Long id : longs) {
            filter.put(id);
        }
        return filter;
    }

    private ImmutableSet<Long> lookupIdForPaAlias(String namespace, Iterable<String> value) {
        Iterable<LookupEntry> entriesForId = lookupStore.entriesForAliases(
                Optional.of(namespace), value
        );
        if (entriesForId.iterator().hasNext()) {
            return ImmutableSet.copyOf(Iterables.transform(entriesForId, LOOKUP_ENTRY_TO_ID));
        }
        return ImmutableSet.of();
    }
}