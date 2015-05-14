package org.atlasapi.remotesite.amazonunbox;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;


public class AmazonUnboxPreProcessingItemProcessor implements AmazonUnboxItemProcessor, AmazonUnboxBrandProcessor {

    private final Map<String, String> contentUriToTitleMap = Maps.newHashMap();
    private final Multimap<String, String> brandUriToSeriesUrisMap = ArrayListMultimap.create();
    private final Multimap<String, String> seriesUriToEpisodeUrisMap = ArrayListMultimap.create();
    private final Map<String, BrandType> brandUriToTypeMap = Maps.newHashMap();
    
    @Override
    public void prepare() {
        contentUriToTitleMap.clear();
        brandUriToSeriesUrisMap.clear();
        seriesUriToEpisodeUrisMap.clear();
        brandUriToTypeMap.clear();
    }
    
    @Override
    public void process(AmazonUnboxItem item) {
        if (AmazonUnboxItem.isBrand(item)) {
            String uri = AmazonUnboxContentExtractor.createBrandUri(item.getAsin());
            contentUriToTitleMap.put(uri, item.getTitle());
        }
        if (AmazonUnboxItem.isSeries(item)) {
            String uri = AmazonUnboxContentExtractor.createSeriesUri(item.getAsin());
            if (item.getSeriesAsin() != null) {
                String brandUri = AmazonUnboxContentExtractor.createBrandUri(item.getAsin());
                brandUriToSeriesUrisMap.put(brandUri, uri);
            }
            contentUriToTitleMap.put(uri, item.getTitle());
        }
        if (AmazonUnboxItem.isEpisode(item)) {
            String uri = AmazonUnboxContentExtractor.createEpisodeUri(item.getAsin());
            if (item.getSeasonAsin() != null) {
                String seriesUri = AmazonUnboxContentExtractor.createSeriesUri(item.getSeasonAsin());
                seriesUriToEpisodeUrisMap.put(seriesUri, uri);
            }
            if (item.getSeriesAsin() != null) {
                String brandUri = AmazonUnboxContentExtractor.createBrandUri(item.getAsin());
                brandUriToSeriesUrisMap.put(brandUri, uri);
            }
            contentUriToTitleMap.put(uri, item.getTitle());
        }
    }

    @Override
    public void finish() {
        for (String brand : brandUriToSeriesUrisMap.keySet()) {
            if (brandUriToSeriesUrisMap.get(brand).size() == 1) {
                String series = Iterables.getOnlyElement(brandUriToSeriesUrisMap.get(brand));
                if (seriesUriToEpisodeUrisMap.get(series).size() == 1) {
                    String episode = Iterables.getOnlyElement(seriesUriToEpisodeUrisMap.get(series));
                    if (contentUriToTitleMap.get(brand).equals(contentUriToTitleMap.get(series))) {
                        if (contentUriToTitleMap.get(brand).equals(contentUriToTitleMap.get(episode))) {
                            brandUriToTypeMap.put(brand, BrandType.STAND_ALONE_EPISODE);
                        } else {
                            brandUriToTypeMap.put(brand, BrandType.TOP_LEVEL_SERIES);
                        }
                    } else {
                        brandUriToTypeMap.put(brand, BrandType.BRAND_SERIES_EPISODE);
                    }
                } else {
                    if (contentUriToTitleMap.get(brand).equals(contentUriToTitleMap.get(series))) {
                        brandUriToTypeMap.put(brand, BrandType.TOP_LEVEL_SERIES);
                    } else {
                        brandUriToTypeMap.put(brand, BrandType.BRAND_SERIES_EPISODE);
                    }
                }
            } else {
                brandUriToTypeMap.put(brand, BrandType.BRAND_SERIES_EPISODE);
            }
        }
    }

    @Override
    public BrandType getBrandType(String uri) {
        return brandUriToTypeMap.get(uri);
    }
}
