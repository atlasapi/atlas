package org.atlasapi.remotesite.btvod;

import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.api.client.util.Maps;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import static org.atlasapi.remotesite.btvod.BtVodProductType.EPISODE;
import static org.atlasapi.remotesite.btvod.BtVodProductType.HELP;


public class BtVodSynthesizedSeriesExtractor extends AbstractBtVodSeriesExtractor {

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

        this.explicitSeriesIds = ImmutableSet.copyOf(explicitSeriesIds);
        synthesizedSeries = Maps.newHashMap();
    }

    private boolean isPartOfSeries(BtVodEntry row) {
        return !HELP.isOfType(row.getProductType())
                && EPISODE.isOfType(row.getProductType())
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
    protected void setAdditionalFields(Series series, BtVodEntry row) {
        series.addAliases(getDescribedFieldsExtractor().synthesisedAliasesFrom(row));
    }

    public Map<String, Series> getSynthesizedSeries() {
        return ImmutableMap.copyOf(synthesizedSeries);
    }
}
