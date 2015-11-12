package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Certificate;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodProductRating;

import com.google.api.client.util.Maps;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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


    public BtVodExplicitSeriesExtractor(
            BtVodBrandProvider btVodBrandProvider,
            Publisher publisher,
            BtVodContentListener listener,
            BtVodDescribedFieldsExtractor describedFieldsExtractor,
            Set<String> processedRows,
            BtVodSeriesUriExtractor seriesUriExtractor,
            BtVodVersionsExtractor versionsExtractor,
            TitleSanitiser titleSanitiser,
            ImageExtractor imageExtractor
    ) {
        super(
                btVodBrandProvider, 
                publisher, 
                listener, 
                processedRows, 
                describedFieldsExtractor, 
                seriesUriExtractor, 
                imageExtractor
             );
        this.titleSanitiser = checkNotNull(titleSanitiser);
        this.versionsExtractor = checkNotNull(versionsExtractor);
        explicitSeries = Maps.newHashMap();
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
        series.addVersions(versionsExtractor.createVersions(row));
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
