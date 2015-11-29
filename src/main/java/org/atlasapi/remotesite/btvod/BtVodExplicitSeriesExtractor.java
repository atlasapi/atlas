package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Certificate;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodProductRating;

import com.google.api.client.util.Maps;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.intl.Countries;

public class BtVodExplicitSeriesExtractor extends AbstractBtVodSeriesExtractor {

    private static final String SERIES_TYPE = "season";

    /**
     * GUID -> series
     */
    private final Map<String, Series> explicitSeries;
    private final BtVodVersionsExtractor versionsExtractor;
    private final TitleSanitiser titleSanitiser;
    private final ImageExtractor imageExtractor;
    private final DedupedDescriptionAndImageSelector descriptionAndImageSelector;

    public BtVodExplicitSeriesExtractor(
            BtVodBrandProvider btVodBrandProvider,
            Publisher publisher,
            BtVodContentListener listener,
            BtVodDescribedFieldsExtractor describedFieldsExtractor,
            Set<String> processedRows,
            BtVodSeriesUriExtractor seriesUriExtractor,
            BtVodVersionsExtractor versionsExtractor,
            TitleSanitiser titleSanitiser,
            ImageExtractor imageExtractor,
            DedupedDescriptionAndImageSelector descriptionAndImageSelector
    ) {
        super(
                btVodBrandProvider, 
                publisher, 
                listener, 
                processedRows, 
                describedFieldsExtractor, 
                seriesUriExtractor
        );
        this.titleSanitiser = checkNotNull(titleSanitiser);
        this.versionsExtractor = checkNotNull(versionsExtractor);
        this.explicitSeries = Maps.newHashMap();
        this.imageExtractor = checkNotNull(imageExtractor);
        this.descriptionAndImageSelector = checkNotNull(descriptionAndImageSelector);
    }

    @Override
    protected boolean shouldProcess(BtVodEntry row) {
        return SERIES_TYPE.equals(row.getProductType());
    }

    @Override
    protected void onSeriesProcessed(Series series, BtVodEntry row) {
        explicitSeries.put(row.getGuid(), series);
    }

    @Override
    protected void setAdditionalFields(Series series, BtVodEntry row) {
        Set<Version> currentVersions = versionsExtractor.createVersions(row);
        Set<Version> existingVersions = series.getVersions();

        if (descriptionAndImageSelector.shouldUpdateDescriptionsAndImages(
                currentVersions, existingVersions)) {
            getDescribedFieldsExtractor().setDescriptionsFrom(row, series);
            setImagesFrom(row, series);
        }

        series.addVersions(currentVersions);

        series.addAliases(getDescribedFieldsExtractor().aliasesFrom(row));
        series.setTitle(titleSanitiser.sanitiseTitle(row.getTitle()));
        getDescribedFieldsExtractor().setDescribedFieldsFrom(row, series);

        BtVodProductRating rating = Iterables.getFirst(row.getplproduct$ratings(), null);
        if (rating != null) {
            series.setCertificates(ImmutableList.of(
                    new Certificate(rating.getProductRating(), Countries.GB)
            ));
        }
    }

    public Map<String, Series> getExplicitSeries() {
        return ImmutableMap.copyOf(explicitSeries);
    }

    private void setImagesFrom(BtVodEntry row, Series series) {
        Set<Image> images = imageExtractor.imagesFor(row);

        if (images.isEmpty()) {
            if (series.getImages() == null) {
                series.setImages(ImmutableSet.<Image>of());
            }
            return;
        }

        series.setImages(images);
        if (series.getImages() != null && !series.getImages().isEmpty()) {
            series.setImage(Iterables.get(series.getImages(), 0).getCanonicalUri());
        }
    }
}
