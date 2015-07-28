package org.atlasapi.remotesite.btvod;

import com.google.api.client.util.Maps;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodProductPricingTier;
import org.atlasapi.remotesite.btvod.model.BtVodProductRating;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class BtVodExplicitSeriesWriter extends AbstractBtVodSeriesWriter {

    private static final String SERIES_TYPE = "season";

    /**
     * GUID -> series
     */
    private final Map<String, Series> explicitSeries;
    private final BtVodVersionsExtractor versionsExtractor;
    private final TitleSanitiser titleSanitiser;


    public BtVodExplicitSeriesWriter(
            ContentWriter writer,
            ContentResolver resolver,
            BtVodBrandWriter brandExtractor,
            Publisher publisher,
            BtVodContentListener listener,
            BtVodDescribedFieldsExtractor describedFieldsExtractor,
            Set<String> processedRows,
            BtVodSeriesUriExtractor seriesUriExtractor,
            BtVodVersionsExtractor versionsExtractor,
            TitleSanitiser titleSanitiser, 
            ImageExtractor imageExtractor
    ) {
        super(writer, resolver, brandExtractor, publisher, listener, processedRows, describedFieldsExtractor, seriesUriExtractor, imageExtractor);
        this.titleSanitiser = titleSanitiser;
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
    }

    public Map<String, Series> getExplicitSeries() {
        return ImmutableMap.copyOf(explicitSeries);
    }
}
