package org.atlasapi.remotesite.btvod;

import static com.google.gson.internal.$Gson$Preconditions.checkNotNull;
import static org.atlasapi.remotesite.btvod.BtVodProductType.COLLECTION;

import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metabroadcast.common.scheduling.UpdateProgress;

public class BtVodCollectionExtractor implements BtVodDataProcessor<UpdateProgress> {

    private static final Logger LOG = LoggerFactory.getLogger(BtVodCollectionExtractor.class);
    private static final boolean CONTINUE = true;

    private final BtVodBrandProvider brandProvider;
    private final ImageExtractor imageExtractor;

    private UpdateProgress progress = UpdateProgress.START;

    public BtVodCollectionExtractor(BtVodBrandProvider brandProvider,
            ImageExtractor imageExtractor) {
        this.brandProvider = checkNotNull(brandProvider);
        this.imageExtractor = checkNotNull(imageExtractor);
    }

    @Override
    public boolean process(BtVodEntry row) {
        UpdateProgress currentProgress = UpdateProgress.FAILURE;

        try {
            if (!shouldProcess(row)) {
                currentProgress = UpdateProgress.SUCCESS;
                return CONTINUE;
            }

            BtVodCollection collection = extractCollection(row);
            brandProvider.updateBrandFromCollection(collection);

            currentProgress = UpdateProgress.SUCCESS;
        }
        catch (Exception e) {
            LOG.warn("Failed to process row: " + row.toString(), e);
        }
        finally {
            progress = progress.reduce(currentProgress);
        }

        return CONTINUE;
    }

    @Override
    public UpdateProgress getResult() {
        return progress;
    }

    private boolean shouldProcess(BtVodEntry row) {
        return COLLECTION.isOfType(row.getProductType());
    }

    private BtVodCollection extractCollection(BtVodEntry row) {
        return new BtVodCollection(
                row.getGuid(),
                new DateTime(row.getAdded(), DateTimeZone.UTC),
                row.getDescription(),
                row.getProductLongDescription(),
                imageExtractor.imagesFor(row)
        );
    }
}
