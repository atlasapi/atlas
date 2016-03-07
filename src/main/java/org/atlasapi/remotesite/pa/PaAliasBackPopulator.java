package org.atlasapi.remotesite.pa;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metabroadcast.common.scheduling.StatusReporter;
import org.atlasapi.equiv.update.tasks.ScheduleTaskProgressStore;
import org.atlasapi.media.entity.*;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;

public class PaAliasBackPopulator {

    private static final Logger LOG = LoggerFactory.getLogger(PaAliasBackPopulator.class);
    private static final String TASK_NAME = PaAliasBackPopulator.class.getCanonicalName();

    private static final int THREADS = 15;
    private final ContentLister contentLister;
    private final ScheduleTaskProgressStore progressStore;
    private final ContentWriter contentWriter;
    private final ListeningExecutorService executor = MoreExecutors.listeningDecorator(
            new ThreadPoolExecutor(
                    THREADS,
                    THREADS,
                    1,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>(THREADS),
                    new ThreadFactoryBuilder().build(),
                    new ThreadPoolExecutor.CallerRunsPolicy()
            )
    );

    public PaAliasBackPopulator(
            ContentLister contentLister,
            ContentWriter contentWriter,
            ScheduleTaskProgressStore progressStore
    ) {
        this.progressStore = checkNotNull(progressStore);
        this.contentLister = checkNotNull(contentLister);
        this.contentWriter = checkNotNull(contentWriter);
    }

    public void backpopulate(StatusReporter reporter, Boolean dryRun) throws Exception {
        Iterator<Content> contentItr = contentLister.listContent(createListingCriteria());
        AtomicInteger mutatedCounter = new AtomicInteger(0);
        AtomicInteger totalCounter = new AtomicInteger(0);
        while (contentItr.hasNext()) {
            Content content = contentItr.next();
            executor.submit(aliasGeneratingRunnable(content, mutatedCounter, dryRun));
            totalCounter.incrementAndGet();
            if (mutatedCounter.get() % 1000 == 0) {
                reporter.reportStatus(String.format("Processed %d total items, added %d aliases", totalCounter.get(), mutatedCounter.get()));
                progressStore.storeProgress(TASK_NAME, ContentListingProgress.progressFrom(content));
            }
        }
    }

    private Runnable aliasGeneratingRunnable(final Content content, final AtomicInteger counter, final Boolean dryRun) {
        return new Runnable() {
            @Override
            public void run() {
                Set<Alias> aliases = content.getAliases();
                if (content instanceof Brand
                        && !aliases.contains(generateAliasFor((Brand) content))) {
                    aliases.add(generateAliasFor((Brand) content));
                    if (!dryRun) {
                        contentWriter.createOrUpdate((Container) content);
                    }
                    counter.incrementAndGet();
                    return;
                }
                if (content instanceof Series
                        && !aliases.contains(generateAliasFor((Series) content))) {
                    aliases.add(generateAliasFor((Series) content));
                    if (!dryRun) {
                        contentWriter.createOrUpdate((Container) content);
                    }
                    counter.incrementAndGet();
                    return;
                }
                if (content instanceof Episode
                        && !aliases.contains(generateAliasFor((Episode) content))) {
                    aliases.add(generateAliasFor((Episode) content));
                    if (!dryRun) {
                        contentWriter.createOrUpdate((Item) content);
                    }
                    counter.incrementAndGet();
                    return;
                }
                if (content instanceof Film
                        && !aliases.contains(generateAliasFor((Film) content))) {
                    aliases.add(generateAliasFor((Film) content));
                    if (!dryRun) {
                        contentWriter.createOrUpdate((Item) content);
                    }
                    counter.incrementAndGet();
                    return;
                }
                if (content instanceof Item && !aliases.contains(generateAliasFor((Item) content))) {
                    aliases.add(generateAliasFor((Item) content));
                    if (!dryRun) {
                        contentWriter.createOrUpdate((Item) content);
                    }
                    counter.incrementAndGet();
                    return;
                }
                LOG.warn("Could not back populate Alias for content of type {} ", content.getClass());
            }
        };
    }

    private Alias generateAliasFor(Brand content) {
        return PaHelper.getBrandAlias(content.getId().toString());
    }

    private Alias generateAliasFor(Series content) {
        return PaHelper.getSeriesAlias(content.getId().toString(), content.getSeriesNumber().toString());
    }

    private Alias generateAliasFor(Episode content) {
        return PaHelper.getEpisodeAlias(content.getId().toString());
    }

    private Alias generateAliasFor(Film content) {
        return PaHelper.getFilmAlias(content.getId().toString());
    }

    private Alias generateAliasFor(Item content) {
        return PaHelper.getAliasItem(content.getId().toString());
    }

    private ContentListingCriteria createListingCriteria() {
        ContentListingProgress progress = progressStore.progressForTask(TASK_NAME);

        return new ContentListingCriteria.Builder()
                .forContent(
                        ContentCategory.TOP_LEVEL_ITEM,
                        ContentCategory.CHILD_ITEM,
                        ContentCategory.CONTAINER
                )
                .forPublisher(Publisher.PA)
                .startingAt(progress != null ? progress : ContentListingProgress.START)
                .build();
    }
}
