package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.common.base.Optional;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

public class BtVodSeriesProvider {

    /**
     * guid -> series
     */
    private final Map<String, Series> explicitSeries;

    /**
     * parentRef, series number -> series
     *
     * Some episodes in MPX should belong to a hierarchy, but do not have a parent guid set.
     * If the respective series was explicitly created from MPX then we will not be able to
     * resolve it. To work around that we have this table to allow for resolving an explicit
     * series by parent ref and series number.
     */
    private final Table<ParentRef, Integer, Series> explicitSeriesTable;

    /**
     * sythesized series uri -> series
     */
    private final Map<String, Series> synthesizedSeries;

    private final BtVodSeriesUriExtractor seriesUriExtractor;
    private final HierarchyDescriptionAndImageUpdater descriptionAndImageUpdater;
    private final CertificateUpdater certificateUpdater;
    private final BtVodBrandProvider brandProvider;
    private final TopicUpdater topicUpdater;
    private final BtVodContentListener listener;

    public BtVodSeriesProvider(
            Map<String, Series> explicitSeries,
            Map<String, Series> synthesizedSeries,
            BtVodSeriesUriExtractor seriesUriExtractor,
            HierarchyDescriptionAndImageUpdater descriptionAndImageUpdater,
            CertificateUpdater certificateUpdater,
            BtVodBrandProvider brandProvider,
            TopicUpdater topicUpdater,
            BtVodContentListener listener
    ) {
        this.explicitSeries = ImmutableMap.copyOf(explicitSeries);
        this.explicitSeriesTable = getSeriesTable(explicitSeries);
        this.synthesizedSeries = ImmutableMap.copyOf(synthesizedSeries);
        this.seriesUriExtractor = checkNotNull(seriesUriExtractor);
        this.descriptionAndImageUpdater = checkNotNull(descriptionAndImageUpdater);
        this.certificateUpdater = checkNotNull(certificateUpdater);
        this.brandProvider = checkNotNull(brandProvider);
        this.topicUpdater = checkNotNull(topicUpdater);
        this.listener = checkNotNull(listener);
    }

    public Optional<Series> seriesFor(BtVodEntry row) {
        if (row.getParentGuid() != null && explicitSeries.containsKey(row.getParentGuid())) {
            return Optional.of(explicitSeries.get(row.getParentGuid()));
        }

        Optional<String> seriesUri = seriesUriExtractor.seriesUriFor(row);
        if (seriesUri.isPresent() && synthesizedSeries.containsKey(seriesUri.get())) {
            return Optional.of(synthesizedSeries.get(seriesUri.get()));
        }

        Optional<ParentRef> parentRef = brandProvider.brandRefFor(row);
        Optional<Integer> seriesNumber = seriesUriExtractor.extractSeriesNumber(row);
        if (parentRef.isPresent() && seriesNumber.isPresent()
                && explicitSeriesTable.contains(parentRef.get(), seriesNumber.get())) {
            return Optional.of(explicitSeriesTable.get(parentRef.get(), seriesNumber.get()));
        }

        return Optional.absent();
    }

    public void updateSeriesFromEpisode(BtVodEntry episodeRow, Episode episode) {
        Optional<Series> seriesOptional = seriesFor(episodeRow);
        if(!seriesOptional.isPresent()) {
            return;
        }
        Series series = seriesOptional.get();

        descriptionAndImageUpdater.update(series, episode);
        certificateUpdater.updateCertificates(series, episode);
        topicUpdater.updateTopics(series, episode.getTopicRefs());

        listener.onContent(series, episodeRow);
    }

    public ImmutableList<Series> getExplicitSeries() {
        return ImmutableList.copyOf(explicitSeries.values());
    }

    public ImmutableList<Series> getSynthesisedSeries() {
        return ImmutableList.copyOf(synthesizedSeries.values());
    }

    private Table<ParentRef, Integer, Series> getSeriesTable(Map<String, Series> seriesMap) {
        HashBasedTable<ParentRef, Integer, Series> table = HashBasedTable.create();

        for (Series series : seriesMap.values()) {
            if (series.getParent() != null && series.getSeriesNumber() != null
                    && !table.contains(series.getParent(), series.getSeriesNumber())) {
                table.put(series.getParent(), series.getSeriesNumber(), series);
            }
        }

        return ImmutableTable.copyOf(table);
    }
}
