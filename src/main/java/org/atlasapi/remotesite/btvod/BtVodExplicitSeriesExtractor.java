package org.atlasapi.remotesite.btvod;

import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Certificate;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodProductRating;

import com.metabroadcast.common.intl.Countries;

import com.google.api.client.util.Maps;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import static com.google.common.base.Preconditions.checkNotNull;

public class BtVodExplicitSeriesExtractor extends AbstractBtVodSeriesExtractor {

    /**
     * GUID -> series
     */
    private final Map<String, Series> explicitSeries;
    private final BtVodVersionsExtractor versionsExtractor;
    private final TitleSanitiser titleSanitiser;
    private final ImageExtractor imageExtractor;
    private final DedupedDescriptionAndImageUpdater descriptionAndImageUpdater;

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
            DedupedDescriptionAndImageUpdater descriptionAndImageUpdater,
            BtVodTagMap btVodTagMap
    ) {
        super(
                btVodBrandProvider, 
                publisher, 
                listener, 
                processedRows, 
                describedFieldsExtractor, 
                seriesUriExtractor,
                btVodTagMap
        );
        this.titleSanitiser = checkNotNull(titleSanitiser);
        this.versionsExtractor = checkNotNull(versionsExtractor);
        this.explicitSeries = Maps.newHashMap();
        this.imageExtractor = checkNotNull(imageExtractor);
        this.descriptionAndImageUpdater = checkNotNull(descriptionAndImageUpdater);
    }

    @Override
    protected boolean shouldProcess(BtVodEntry row) {
        return BtVodProductType.SEASON.isOfType(row.getProductType());
    }

    @Override
    protected void onSeriesProcessed(Series series, BtVodEntry row) {
        explicitSeries.put(row.getGuid(), series);
    }

    @Override
    protected void setAdditionalFields(Series series, BtVodEntry row) {
        Set<Version> currentVersions = versionsExtractor.createVersions(row);

        descriptionAndImageUpdater.updateDescriptionsAndImages(
                series, row, imageExtractor.imagesFor(row), currentVersions
        );

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
}
