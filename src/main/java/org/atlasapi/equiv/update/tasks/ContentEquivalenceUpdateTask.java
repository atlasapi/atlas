package org.atlasapi.equiv.update.tasks;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.equiv.ColumbusTelescopeReporter;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.RootEquivalenceUpdater;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.content.listing.ContentListingProgress;

import com.metabroadcast.common.scheduling.UpdateProgress;

import com.metabroadcast.columbus.telescope.api.Environment;
import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;
import com.metabroadcast.common.scheduling.UpdateProgress;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import static com.metabroadcast.common.scheduling.UpdateProgress.FAILURE;
import static com.metabroadcast.common.scheduling.UpdateProgress.SUCCESS;
import static org.atlasapi.persistence.content.ContentCategory.CONTAINER;
import static org.atlasapi.persistence.content.ContentCategory.TOP_LEVEL_ITEM;
import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;
import static org.atlasapi.persistence.content.listing.ContentListingProgress.progressFrom;

public final class ContentEquivalenceUpdateTask extends AbstractContentListingTask<Content> {

    private final ScheduleTaskProgressStore progressStore;
    private final EquivalenceUpdater<Content> updater;    
    private final Set<String> ignored;

    private String schedulingKey = "equivalence";
    private List<Publisher> publishers;
    private int processed = 0;
    private UpdateProgress progress = UpdateProgress.START;

    private final Environment reportingEnvironment;
    private final IngestTelescopeClientImpl telescopeClient;
    private final ColumbusTelescopeReporter reporter;
    private Optional<String> taskId;

    public ContentEquivalenceUpdateTask(
            ContentLister contentLister,
            ContentResolver contentResolver,
            ScheduleTaskProgressStore progressStore,
            EquivalenceUpdater<Content> updater,
            Set<String> ignored,
            Environment reportingEnvironment,
            IngestTelescopeClientImpl telescopeClient
    ) {
        super(contentLister);
        this.progressStore = progressStore;
        this.updater = RootEquivalenceUpdater.create(contentResolver, updater);
        this.ignored = ignored;
        this.reportingEnvironment = reportingEnvironment;
        this.telescopeClient = telescopeClient;
        this.reporter = new ColumbusTelescopeReporter(telescopeClient);
    }

    public ContentEquivalenceUpdateTask forPublishers(Publisher... publishers) {
        this.publishers = ImmutableList.copyOf(publishers);
        this.schedulingKey = Joiner.on("-").join(Iterables.transform(this.publishers, Publisher.TO_KEY))+"-equivalence";
        return this;
    }

    @Override
    protected ContentListingProgress getProgress() {
        return progressStore.progressForTask(schedulingKey);
    }

    @Override
    protected Iterator<Content> filter(Iterator<Content> rawIterator) {
        return rawIterator;
    }

    @Override
    protected ContentListingCriteria listingCriteria(ContentListingProgress progress) {
        return defaultCriteria().forPublishers(publishers).forContent(CONTAINER, TOP_LEVEL_ITEM).startingAt(progress).build();
    }
    
    @Override
    protected void onStart(ContentListingProgress progress) {
        log.info("Started: {} from {}", schedulingKey, describe(progress));

        this.taskId = reporter.startReporting(
                progress.getPublisher(),
                reportingEnvironment
        );

        processed  = 0;
        this.progress = UpdateProgress.START;
    }

    private String describe(ContentListingProgress progress) {
        if (progress == null || ContentListingProgress.START.equals(progress)) {
            return "start";
        }
        return String.format("%s %s %s", progress.getCategory(), progress.getPublisher(), progress.getUri());
    }
    
    @Override
    protected boolean handle(Content content) {
        if (!ignored.contains(content.getCanonicalUri())) {
            reportStatus(String.format("%s. Processing %s.", progress, content));
            try {
                updater.updateEquivalences(content, taskId, telescopeClient);
                progress = progress.reduce(SUCCESS);
            } catch (Exception e) {
                log.error(content.toString(), e);
                progress = progress.reduce(FAILURE);
            }
            if (++processed % 10 == 0) {
                updateProgress(progressFrom(content));
            }
        }
        return true;
    }

    @Override
    protected void onFinish(boolean finished, @Nullable Content lastProcessed) {
        reporter.endReporting(taskId);
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
