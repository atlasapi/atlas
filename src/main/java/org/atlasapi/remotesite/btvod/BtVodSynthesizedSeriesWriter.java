package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Set;

import com.google.api.client.util.Maps;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;


public class BtVodSynthesizedSeriesWriter extends AbstractBtVodSeriesWriter {


    private static final String HELP_TYPE = "help";
    private static final String EPISODE_TYPE = "episode";

    /**
     * GUID -> series
     */
    private final Map<String, Series> synthesizedSeries;

    private final Set<String> explicitSeriesIds;
    public BtVodSynthesizedSeriesWriter(
            ContentWriter writer,
            ContentResolver resolver,
            BtVodBrandWriter brandExtractor,
            Publisher publisher,
            BtVodContentListener listener,
            BtVodDescribedFieldsExtractor describedFieldsExtractor,
            Set<String> processedRows,
            BtVodSeriesUriExtractor seriesUriExtractor,
            Set<String> explicitSeriesIds) {
        super(
                writer,
                resolver,
                brandExtractor,
                publisher,
                listener,
                processedRows,
                describedFieldsExtractor,
                seriesUriExtractor
        );

        this.explicitSeriesIds = ImmutableSet.copyOf(explicitSeriesIds);
        synthesizedSeries = Maps.newHashMap();
    }



    private boolean isPartOfSeries(BtVodEntry row) {
        return !HELP_TYPE.equals(row.getProductType())
                && EPISODE_TYPE.equals(row.getProductType())
                && getSeriesUriExtractor().extractSeriesNumber(row).isPresent();
    }

    @Override
    protected boolean shouldProcess(BtVodEntry row) {
        String parentGuid = row.getParentGuid();
        if (parentGuid == null) {
            return isPartOfSeries(row);
        }
        return !explicitSeriesIds.contains(parentGuid) && isPartOfSeries(row);
    }

    @Override
    protected void onSeriesProcessed(Series series, BtVodEntry row) {
        if (synthesizedSeries.containsKey(series.getCanonicalUri())) {
            synthesizedSeries.put(series.getCanonicalUri(), series);
        }
    }

    @Override
    protected void setAdditionalFields(Series series, BtVodEntry row) {}

    public Map<String, Series> getSynthesizedSeries() {
        return ImmutableMap.copyOf(synthesizedSeries);
    }
}
