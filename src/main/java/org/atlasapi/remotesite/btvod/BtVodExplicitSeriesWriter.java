package org.atlasapi.remotesite.btvod;

import com.google.api.client.util.Maps;
import com.google.common.collect.ImmutableMap;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class BtVodExplicitSeriesWriter extends AbstractBtVodSeriesWriter {

    private static final String SERIES_TYPE = "season";

    /**
     * GUID -> series
     */
    private final Map<String, Series> explicitSeries;

    public BtVodExplicitSeriesWriter(
            ContentWriter writer,
            ContentResolver resolver,
            BtVodBrandWriter brandExtractor,
            Publisher publisher,
            BtVodContentListener listener,
            BtVodDescribedFieldsExtractor describedFieldsExtractor,
            Set<String> processedRows,
            BtVodSeriesUriExtractor seriesUriExtractor

            ) {
        super(writer, resolver, brandExtractor, publisher, listener, processedRows, describedFieldsExtractor, seriesUriExtractor);
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

    public Map<String, Series> getExplicitSeries() {
        return ImmutableMap.copyOf(explicitSeries);
    }
}
