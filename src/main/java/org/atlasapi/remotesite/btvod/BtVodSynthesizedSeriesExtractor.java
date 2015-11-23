package org.atlasapi.remotesite.btvod;

import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.api.client.util.Maps;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


public class BtVodSynthesizedSeriesExtractor extends AbstractBtVodSeriesExtractor {


    private static final String HELP_TYPE = "help";
    private static final String EPISODE_TYPE = "episode";

    /**
     * GUID -> series
     */
    private final Map<String, Series> synthesizedSeries;

    private final Set<String> explicitSeriesIds;
    public BtVodSynthesizedSeriesExtractor(
            BtVodBrandProvider btVodBrandProvider,
            Publisher publisher,
            BtVodContentListener listener,
            BtVodDescribedFieldsExtractor describedFieldsExtractor,
            Set<String> processedRows,
            BtVodSeriesUriExtractor seriesUriExtractor,
            Set<String> explicitSeriesIds,
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

        this.explicitSeriesIds = ImmutableSet.copyOf(explicitSeriesIds);
        synthesizedSeries = Maps.newHashMap();
    }

    private boolean isPartOfSeries(BtVodEntry row) {
        return !HELP_TYPE.equals(row.getProductType())
                && EPISODE_TYPE.equals(row.getProductType())
                && getSeriesUriExtractor().extractSeriesNumber(row).isPresent();

    }

    private boolean isAlreadyProcessed(BtVodEntry row) {
        return getSeriesUriExtractor().seriesUriFor(row).isPresent()
                && getProcessedRows().contains(getSeriesUriExtractor().seriesUriFor(row).get());
    }

    @Override
    protected boolean shouldProcess(BtVodEntry row) {
        String parentGuid = row.getParentGuid();
        if (parentGuid == null) {
            return isPartOfSeries(row) && !isAlreadyProcessed(row);
        }
        return isPartOfSeries(row) && !isAlreadyProcessed(row) && !explicitSeriesIds.contains(parentGuid);
    }

    @Override
    protected void onSeriesProcessed(Series series, BtVodEntry row) {
        if (!synthesizedSeries.containsKey(series.getCanonicalUri())) {
            synthesizedSeries.put(series.getCanonicalUri(), series);
        }
    }

    @Override
    protected void setAdditionalFields(Series series, BtVodEntry row) {}

    public Map<String, Series> getSynthesizedSeries() {
        return ImmutableMap.copyOf(synthesizedSeries);
    }
}
