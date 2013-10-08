package org.atlasapi.remotesite.amazonunbox;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;


public class AmazonUnboxPreProcessingItemProcessor implements AmazonUnboxItemProcessor, AmazonUnboxBrandProcessor {

    private final Map<String, String> uriTitleMapping = Maps.newHashMap();
    private final Multimap<String, String> brandSeriesMapping = ArrayListMultimap.create();
    private final Multimap<String, String> seriesEpisodeMapping = ArrayListMultimap.create();
    private final Map<String, BrandType> brandTypeMapping = Maps.newHashMap();
    
    @Override
    public void prepare() {
        uriTitleMapping.clear();
        brandSeriesMapping.clear();
        seriesEpisodeMapping.clear();
        brandTypeMapping.clear();
    }
    
    @Override
    public void process(AmazonUnboxItem item) {
        if (AmazonUnboxItem.isBrand(item)) {
            String uri = AmazonUnboxContentExtractor.createBrandUri(item.getAsin());
            uriTitleMapping.put(uri, item.getTitle());
        }
        if (AmazonUnboxItem.isSeries(item)) {
            String uri = AmazonUnboxContentExtractor.createSeriesUri(item.getAsin());
            if (item.getSeriesAsin() != null) {
                String brandUri = AmazonUnboxContentExtractor.createBrandUri(item.getAsin());
                brandSeriesMapping.put(brandUri, uri);
            }
            uriTitleMapping.put(uri, item.getTitle());
        }
        if (AmazonUnboxItem.isEpisode(item)) {
            String uri = AmazonUnboxContentExtractor.createEpisodeUri(item.getAsin());
            if (item.getSeasonAsin() != null) {
                String seriesUri = AmazonUnboxContentExtractor.createSeriesUri(item.getSeasonAsin());
                seriesEpisodeMapping.put(seriesUri, uri);
            }
            if (item.getSeriesAsin() != null) {
                String brandUri = AmazonUnboxContentExtractor.createBrandUri(item.getAsin());
                brandSeriesMapping.put(brandUri, uri);
            }
            uriTitleMapping.put(uri, item.getTitle());
        }
    }

    @Override
    public void finish() {
        for (String brand : brandSeriesMapping.keySet()) {
            if (brandSeriesMapping.get(brand).size() == 1) {
                String series = Iterables.getOnlyElement(brandSeriesMapping.get(brand));
                if (seriesEpisodeMapping.get(series).size() == 1) {
                    String episode = Iterables.getOnlyElement(seriesEpisodeMapping.get(series));
                    if (uriTitleMapping.get(brand).equals(uriTitleMapping.get(series))) {
                        if (uriTitleMapping.get(brand).equals(uriTitleMapping.get(episode))) {
                            brandTypeMapping.put(brand, BrandType.STAND_ALONE_EPISODE);
                        } else {
                            brandTypeMapping.put(brand, BrandType.TOP_LEVEL_SERIES);
                        }
                    } else {
                        brandTypeMapping.put(brand, BrandType.BRAND_SERIES_EPISODE);
                    }
                } else {
                    if (uriTitleMapping.get(brand).equals(uriTitleMapping.get(series))) {
                        brandTypeMapping.put(brand, BrandType.TOP_LEVEL_SERIES);
                    } else {
                        brandTypeMapping.put(brand, BrandType.BRAND_SERIES_EPISODE);
                    }
                }
            } else {
                brandTypeMapping.put(brand, BrandType.BRAND_SERIES_EPISODE);
            }
        }
    }

    @Override
    public BrandType getBrandType(String uri) {
        return brandTypeMapping.get(uri);
    }
}
