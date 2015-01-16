package org.atlasapi.remotesite.getty;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;

public class GettyUpdateTask extends ScheduledTask {
    private static final Logger log = LoggerFactory.getLogger(GettyUpdateTask.class);

    static final String JOB_KEY = "getty-ingest";

    private final GettyClient gettyClient;
    private final GettyAdapter adapter;
    private final GettyDataHandler dataHandler;
    private final ContentLister contentLister;

    private final int itemsPerPage;
    private final String idsFileName;
    private final RestartStatusSupplier restartStatus;

    public GettyUpdateTask(GettyClient gettyClient, GettyAdapter adapter,
            ContentLister contentLister,
            GettyDataHandler dataHandler, String idsFileName,
            int itemsPerPage, RestartStatusSupplier restartStatus) {
        this.gettyClient = checkNotNull(gettyClient);
        this.adapter = checkNotNull(adapter);
        this.dataHandler = checkNotNull(dataHandler);
        this.contentLister = checkNotNull(contentLister);

        this.itemsPerPage = itemsPerPage;
        this.idsFileName = idsFileName;
        this.restartStatus = checkNotNull(restartStatus);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void runTask() {
        UpdateProgress progress = UpdateProgress.START;
        final Optional<Integer> firstOffset = restartStatus.startFromOffset();

        reportStatus("Reading ID-list file.");
        ImmutableList<String> gettyIds = ImmutableList.of();
        try {
            gettyIds = ImmutableList.<String>copyOf(IOUtils.readLines(new FileReader(idsFileName)));
        } catch (Exception e) {
            Throwables.propagate(e);
        }

        int removals = 0;
        if (! firstOffset.isPresent()) {
            Set<String> expectedGettyUris = ImmutableSet.copyOf(Iterables.transform(gettyIds, new Function<String, String>() {
                @Override public String apply(String id) {
                    return GettyContentExtractor.uri(id);
                }
            }));

            reportStatus("File contains " + gettyIds.size() + " IDs. Now unpublishing disappeared content.");
            Iterator<Content> allStoredGettyContent = contentLister.listContent(ContentListingCriteria.defaultCriteria().forContent(ContentCategory.TOP_LEVEL_ITEM).forPublishers(Publisher.GETTY).build());
            while (allStoredGettyContent.hasNext()) {
                Content item = allStoredGettyContent.next();
                if (! expectedGettyUris.contains(item.getCanonicalUri())) {
                    ++removals;
                    log.info("Unpublishing item {}", item.getCanonicalUri());
                    item.setActivelyPublished(false);
                    dataHandler.write(item);
                }
            }
        }

        int offset = firstOffset.or(0);

        // Page through videos and merge in resulting content
        while (shouldContinue() && offset < gettyIds.size()) {
            try {
                restartStatus.saveCurrentOffset(offset);
                ImmutableList<String> thisPageIds = gettyIds.subList(offset, Math.min(offset + itemsPerPage, gettyIds.size()));
                reportStatus(removals + " removals. Processing content. " + progress.toString() + ". " + gettyIds.size() + " total, started at offset " + firstOffset.or(0) + ".");

                String response = null;
                try {
                    log.debug("Requesting Getty items from {}", offset);
                    response = gettyClient.getVideoResponse(thisPageIds);
                } catch (IOException e) {
                    Throwables.propagate(e);
                }

                log.debug("Parsing response");
                List<VideoResponse> videos = adapter.parse(response);

                for (VideoResponse video : videos) {
                    try {
                        log.debug("Writing item {} ({})", video.getAssetId(), video.getTitle());
                        Identified written = dataHandler.handle(video);

                        progress = progress.reduce(UpdateProgress.SUCCESS);
                    } catch (Exception e) {
                        log.warn(String.format("Failed to interpret a video response"), e);
                        progress = progress.reduce(UpdateProgress.FAILURE);
                    }
                }
            } catch (Exception e) {
                log.error("Whole batch failed for some reason", e);
            }
            offset += itemsPerPage;
        }

        restartStatus.clearCurrentOffset();
        reportStatus("Finished. " + removals + " removals." + progress.toString() + ". Expected " + gettyIds.size() +" items.");
    }

}
