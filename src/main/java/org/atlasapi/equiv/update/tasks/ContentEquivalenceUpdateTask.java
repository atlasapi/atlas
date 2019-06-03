package org.atlasapi.equiv.update.tasks;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.RootEquivalenceUpdater;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.content.listing.SelectedContentLister;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporterFactory;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;

import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.metabroadcast.common.scheduling.UpdateProgress.FAILURE;
import static com.metabroadcast.common.scheduling.UpdateProgress.SUCCESS;
import static org.atlasapi.persistence.content.ContentCategory.CONTAINER;
import static org.atlasapi.persistence.content.ContentCategory.TOP_LEVEL_ITEM;
import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;
import static org.atlasapi.persistence.content.listing.ContentListingProgress.progressFrom;

public final class ContentEquivalenceUpdateTask extends ScheduledTask {
    protected final Logger log = LoggerFactory.getLogger(ContentEquivalenceUpdateTask.class);

    /**
     * If it has been initialized, it will be used. If it hasn't been initialized everything will
     * run on the callers thread.
     */
    private final @Nullable ExecutorService executor;
    private final SelectedContentLister contentLister;
    private final ScheduleTaskProgressStore progressStore;
    private final EquivalenceUpdater<Content> updater;
    private final ContentResolver contentResolver;
    private final Set<String> ignored;
    private final OwlTelescopeReporter telescope;

    public static final int SAVE_EVERY_BLOCK_SIZE = 50;

    private String schedulingKey = "equivalence";
    private List<Publisher> publishers;
    private UpdateProgress progress = UpdateProgress.START;

    public ContentEquivalenceUpdateTask(SelectedContentLister contentLister,
            ContentResolver contentResolver, ScheduleTaskProgressStore progressStore,
            EquivalenceUpdater<Content> updater, Set<String> ignored) {
        this(contentLister, contentResolver, null, progressStore, updater, ignored);
    }

    /**
     * @param executor        If the executor is null all equiv will run on the callers thread.
     */
    public ContentEquivalenceUpdateTask(
            SelectedContentLister contentLister,
            ContentResolver contentResolver,
            @Nullable ExecutorService executor,
            ScheduleTaskProgressStore progressStore,
            EquivalenceUpdater<Content> updater,
            Set<String> ignored) {
        this.contentLister = contentLister;
        this.executor = executor;
        this.progressStore = progressStore;
        this.updater = RootEquivalenceUpdater.create(contentResolver, updater);
        this.contentResolver = contentResolver;
        this.ignored = ignored;
        this.telescope = OwlTelescopeReporterFactory.getInstance().getTelescopeReporter(
                OwlTelescopeReporters.EQUIVALENCE,
                Event.Type.EQUIVALENCE
        );
    }

    public ContentEquivalenceUpdateTask forPublishers(Publisher... publishers) {
        this.publishers = ImmutableList.copyOf(publishers);
        this.schedulingKey = Joiner.on("-").join(this.publishers.stream()
                .map(Publisher.TO_KEY::apply)
                .collect(Collectors.toList()))+"-equivalence";
        return this;
    }

    protected ContentListingProgress getProgress() {
        return progressStore.progressForTask(schedulingKey);
    }

    protected Iterator<Content> filter(Iterator<Content> rawIterator) {
        return rawIterator;
    }

    protected ContentListingCriteria listingCriteria(ContentListingProgress progress) {
        return defaultCriteria().forPublishers(publishers).forContent(CONTAINER, TOP_LEVEL_ITEM).startingAt(progress).build();
    }

    @Override
    protected void runTask() {

        ContentListingProgress progress = getProgress();

        onStart(progress);

        List<String> contentUris = contentLister.listContentUris(listingCriteria(progress));

        log.info("Running equiv on all content from {}", progress.getPublisher());
        if (executor == null) {
            runSynchronously(contentUris);
        } else {
            runAsynchronously(contentUris);
        }
    }

    private void runAsynchronously(List<String> uris) {
        String currentUri = null;
        Content currentContent = null;
        try {
            while (shouldContinue() && !uris.isEmpty()) {
                //Normally this saves progress to the db every 10 items. With multithreading
                //we need to make sure that when progress is written, everything before that item
                //has completed successfully. The strategy chosen was to batch tasks into
                //blocks of SAVE_EVERY_BLOCK_SIZE, add them to the executor, wait for the whole
                //batch to finish, write progress, repeat until done.
                CountDownLatch latch = new CountDownLatch(SAVE_EVERY_BLOCK_SIZE);
                int submitted = 0;
                while (shouldContinue() && !uris.isEmpty() && submitted < SAVE_EVERY_BLOCK_SIZE) {
                    currentUri = uris.get(0);
                    uris.remove(0);
                    //We don't check the result of handle, because that was always true. If you
                    //ever need to check that result you probably need to refactor the code
                    //using invokeAll instead of the countdown latch.
                    currentContent = resolveUri(currentUri);
                }
                executor.submit(handleAsync(currentContent, latch));
                submitted++;

                //reduce the latch by the difference between the wanted amount,
                // and the amount we managed to submit. (i.e. manage the last few).
                while (submitted < SAVE_EVERY_BLOCK_SIZE) {
                    latch.countDown();
                    submitted++;
                }
                //wait for the block to finish, write progress in the db, continue with next block.
                latch.await();
                updateProgress(progressFrom(currentContent));
            }
        } catch (Exception e) {
            log.error(getName(), e);
            onFinish(false, null);  //pass null so as to not persist state
            Throwables.propagate(e);    //in order to set the task as failed
        }
        onFinish(shouldContinue(), null);
    }

    private void runSynchronously(List<String> uris) {
        boolean proceed = true;
        String currentUri = null;
        Content currentContent = null;
        int processed = 0;
        try {
            while (shouldContinue() && proceed && !uris.isEmpty()) {
                currentUri = uris.get(0);
                uris.remove(0);
                currentContent = resolveUri(currentUri);
                proceed = handle(currentContent);
                if (++processed % 10 == 0) {
                    updateProgress(progressFrom(currentContent));
                }
            }
        } catch (Exception e) {
            log.error(getName(), e);
            onFinish(false, currentContent);
            throw e;
        }
        onFinish(proceed && shouldContinue(), currentContent);
    }

    private boolean isContent(Maybe<Identified> possibleContent) {
        return possibleContent.valueOrNull() instanceof Content;
    }

    private Content resolveUri(String currentUri) {
        ResolvedContent resolvedContent = contentResolver.findByCanonicalUris(Collections
                .singleton(currentUri));
        Maybe<Identified> possibleContent = resolvedContent.get(currentUri);
        return isContent(possibleContent)
               ? (Content) possibleContent.requireValue()
               : null;    //shouldn't happen; should always be found as uris were taken from db
    }

    protected void onStart(ContentListingProgress progress) {
        telescope.startReporting();
        log.info("Started: {} from {}", schedulingKey, describe(progress));
        this.progress = UpdateProgress.START;
    }

    private String describe(ContentListingProgress progress) {
        if (progress == null || ContentListingProgress.START.equals(progress)) {
            return "start";
        }
        return String.format("%s %s %s", progress.getCategory(), progress.getPublisher(), progress.getUri());
    }

    protected boolean handle(Content content) {
        if (!ignored.contains(content.getCanonicalUri())) {
            reportStatus(String.format("%s. Processing %s.", progress, content));
            try {
                updater.updateEquivalences(content, telescope);
                progress = progress.reduce(SUCCESS);
            } catch (Exception e) {
                log.error(content.toString(), e);
                progress = progress.reduce(FAILURE);
                return false;
            }
        }
        return true;
    }


    protected Runnable handleAsync(Content content, CountDownLatch latch){
        return () -> {handle(content); latch.countDown();};
    }

    protected void onFinish(boolean finished, @Nullable Content lastProcessed) {
        telescope.endReporting();
        persistProgress(finished, lastProcessed);
    }

    private void persistProgress(boolean finished, Content content) {
        if (finished) {
            updateProgress(ContentListingProgress.START);
            log.info("Finished: {}", schedulingKey);
        } else {
            if (content != null) {
                updateProgress(progressFrom(content));
                log.info("Stopped: {}", schedulingKey, content);
            }
        }
    }

    private void updateProgress(ContentListingProgress progress) {
        progressStore.storeProgress(schedulingKey, progress);
    }

}
