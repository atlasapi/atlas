package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class BtVodBrandProvider {

    private final BrandUriExtractor brandUriExtractor;
    private final Map<String, Brand> brands;
    private final DescriptionAndImageUpdater descriptionAndImageUpdater;
    private final CertificateUpdater certificateUpdater;
    private final TopicUpdater topicUpdater;
    private final BtVodContentListener listener;

    public BtVodBrandProvider(
            BrandUriExtractor brandUriExtractor,
            Map<String, Brand> brands,
            DescriptionAndImageUpdater descriptionAndImageUpdater,
            CertificateUpdater certificateUpdater,
            TopicUpdater topicUpdater,
            BtVodContentListener listener
    ) {
        this.brandUriExtractor = checkNotNull(brandUriExtractor);
        this.brands = ImmutableMap.copyOf(brands);
        this.descriptionAndImageUpdater = checkNotNull(descriptionAndImageUpdater);
        this.certificateUpdater = checkNotNull(certificateUpdater);
        this.topicUpdater = checkNotNull(topicUpdater);
        this.listener = checkNotNull(listener);
    }


    public Optional<Brand> brandFor(BtVodEntry row) {
        Optional<String> brandUri = brandUriExtractor.extractBrandUri(row);
        if (brandUri.isPresent() && brands.containsKey(brandUri.get())) {
            return Optional.of(brands.get(brandUri.get()));
        }
        return Optional.absent();

    }

    public Optional<ParentRef> brandRefFor(BtVodEntry row) {
        Optional<String> optionalUri = brandUriExtractor.extractBrandUri(row);

        if (!optionalUri.isPresent() || !brands.containsKey(optionalUri.get())) {
            return Optional.absent();
        }

        return Optional.of(ParentRef.parentRefFrom(brands.get(optionalUri.get())));
    }

    public void updateBrandFromSeries(BtVodEntry seriesRow, Series series) {
        Optional<Brand> brandOptional = brandFor(seriesRow);
        if(!brandOptional.isPresent()) {
            return;
        }
        Brand brand = brandOptional.get();

        descriptionAndImageUpdater.update(brand, series);
        certificateUpdater.updateCertificates(brand, series);
        topicUpdater.updateTopics(brand, series.getTopicRefs());

        listener.onContent(brand, seriesRow);
    }

    public void updateBrandFromEpisode(BtVodEntry episodeRow, Episode episode) {
        Optional<Brand> brandOptional = brandFor(episodeRow);
        if(!brandOptional.isPresent()) {
            return;
        }
        Brand brand = brandOptional.get();

        descriptionAndImageUpdater.update(brand, episode);
        certificateUpdater.updateCertificates(brand, episode);
        topicUpdater.updateTopics(brand, episode.getTopicRefs());

        listener.onContent(brand, episodeRow);
    }

    public ImmutableList<Brand> getBrands() {
        return ImmutableList.copyOf(brands.values());
    }
}
