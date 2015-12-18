package org.atlasapi.remotesite.itunes.epf;

import static org.atlasapi.persistence.logging.AdapterLogEntry.errorEntry;
import static org.atlasapi.remotesite.itunes.epf.EpfHelper.curieForBrand;
import static org.atlasapi.remotesite.itunes.epf.EpfHelper.uriForBrand;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.remotesite.itunes.epf.model.ArtistType;
import org.atlasapi.remotesite.itunes.epf.model.CollectionType;
import org.atlasapi.remotesite.itunes.epf.model.EpfArtist;
import org.atlasapi.remotesite.itunes.epf.model.EpfArtistCollection;
import org.atlasapi.remotesite.itunes.epf.model.EpfCollection;
import org.atlasapi.remotesite.itunes.epf.model.EpfCollectionVideo;
import org.atlasapi.remotesite.itunes.epf.model.EpfPricing;
import org.atlasapi.remotesite.itunes.epf.model.EpfVideo;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;
import com.metabroadcast.common.scheduling.ScheduledTask;

public class ItunesEpfUpdateTask extends ScheduledTask {

    private final Supplier<EpfDataSet> dataSetSupplier;
    private final ContentWriter writer;

    private final ItunesCollectionSeriesExtractor seriesExtractor = new ItunesCollectionSeriesExtractor();
    private final ItunesVideoEpisodeExtractor episodeExtractor = new ItunesVideoEpisodeExtractor();
    private final ItunesPricingLocationExtractor locationExtractor = new ItunesPricingLocationExtractor();
    private final AdapterLog log;
    
    public ItunesEpfUpdateTask(Supplier<EpfDataSet> dataSetSupplier, ContentWriter writer, AdapterLog log) {
        this.dataSetSupplier = dataSetSupplier;
        this.writer = writer;
        this.log = log;
    }

    @Override
    protected void runTask() {
        try {
            EpfDataSet dataSet = dataSetSupplier.get();

            //brand id -> brand
            final BiMap<Integer, Brand> extractedBrands = extractBrands(dataSet.getArtistTable());
            
            int brands = 0;
            for (Brand brand : extractedBrands.values()) {
                writer.createOrUpdate(brand);
                reportStatus(String.format("Writing brands %s/%s", ++brands, extractedBrands.size()));
            }

            //series id -> series
            final BiMap<Integer, Series> extractedSeries = linkBrandsAndSeries(dataSet.getArtistCollectionTable(), extractedBrands, extractSeries(dataSet.getCollectionTable()));
            
            Multimap<String, Location> extractedLocations = extractLocations(dataSet, ImmutableSet.of(Countries.GB, Countries.US));
            
            //episode id -> trackNumber/series
            Multimap<Series, Episode> extractedEpisodes = linkEpisodesAndSeries(dataSet.getCollectionVideoTable(), extractedSeries, extractVideos(dataSet.getVideoTable(), extractedSeries, extractedLocations));
            
            int seriess = 0;
            Set<Series> seriesToWrite = extractedEpisodes.keySet();
            for (Series series : seriesToWrite) {
                writer.createOrUpdate(series);
                reportStatus(String.format("Writing series %s/%s", ++seriess, seriesToWrite.size()));
            }

            int episodes = 0;
            for (Episode episode : extractedEpisodes.values()) {
                writer.createOrUpdate(episode);
                reportStatus(String.format("Writing episodes %s/%s", ++episodes, extractedEpisodes.size()));
            }
            
        } catch (Exception e) {
            log.record(errorEntry().withCause(e).withDescription("Error during EPF update").withSource(getClass()));
            throw Throwables.propagate(e);
        }
    }

    // Return a map here to give this function some transparency.
    private BiMap<Integer, Series> linkBrandsAndSeries(EpfTable<EpfArtistCollection> artistCollectionTable, final Map<Integer, Brand> brands, final BiMap<Integer, Series> series) throws IOException {
        reportStatus("Linking series to brands");
        return artistCollectionTable.processRows(new EpfTableRowProcessor<EpfArtistCollection, BiMap<Integer, Series>>() {

            private final ImmutableBiMap.Builder<Integer, Series> linkedSeries = ImmutableBiMap.builder();
            
            @Override
            public boolean process(EpfArtistCollection row) {
                Brand brand = brands.get(row.get(EpfArtistCollection.ARTIST_ID));
                if(brand != null) {
                    Integer collectionId = row.get(EpfArtistCollection.COLLECTION_ID);
                    Series sery = series.get(collectionId);
                    if (sery != null) {
                        if (!linkedSeries.build().containsKey(collectionId)) {
                            sery.setParent(brand);
                            linkedSeries.put(collectionId, sery);
                        }
                    }

                }
                return true;
            }

            @Override
            public BiMap<Integer, Series> getResult() {
                return linkedSeries.build();
            }
        });
    }

    private Multimap<String, Location> extractLocations(final EpfDataSet dataSet, ImmutableSet<Country> countries) throws IOException {
        reportStatus("Extracting locations...");
        Iterable<Location> locations = Iterables.concat(Iterables.transform(countries, new Function<Country, Set<Location>>() {
            @Override
            public Set<Location> apply(final Country country) {
                try {
                    EpfTable<EpfPricing> pricingTable = dataSet.getPricingTable(country);
                    if (pricingTable == null) {
                        return ImmutableSet.of();
                    }
                    return pricingTable.processRows(new EpfTableRowProcessor<EpfPricing, Set<Location>>() {

                        private final ImmutableSet.Builder<Location> countryLocations = ImmutableSet.builder();

                        @Override
                        public boolean process(EpfPricing row) {
                            Maybe<Location> location = locationExtractor.extract(new ItunesEpfPricingSource(row, country));
                            if (location.hasValue()) {
                                countryLocations.add(location.requireValue());
                            }
                            return isRunning();
                        }

                        @Override
                        public Set<Location> getResult() {
                            return countryLocations.build();
                        }
                    });
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        }));
        
        return Multimaps.index(locations, new Function<Location, String>() {
            
            private final String commonPrefix = "http://itunes.apple.com/WebObjects/MZStore.woa/wa/viewTVSeason?";
            private final Pattern urlIdPattern = Pattern.compile(Pattern.quote(commonPrefix) + "uo=\\d+&i=(\\d+)&id=\\d+");
            
            @Override
            public String apply(Location input) {
                return extractIdFrom(input.getUri());
            }

            private String extractIdFrom(String uri) {
                Matcher matcher = urlIdPattern.matcher(uri);
                if(matcher.matches()) {
                    return matcher.group(1);
                }
                return "";
            }
        });
    }

    private Map<Integer, Episode> extractVideos(EpfTable<EpfVideo> videosTable, final BiMap<Integer, Series> extractedSeries, final Multimap<String, Location> extractedLocations) throws IOException {
        reportStatus("Extracting episodes");
        return videosTable.processRows(new EpfTableRowProcessor<EpfVideo, Map<Integer,Episode>>() {

            ImmutableMap.Builder<Integer, Episode> results = ImmutableMap.builder();
            
            @Override
            public boolean process(EpfVideo row) {
                Integer videoId = row.get(EpfVideo.VIDEO_ID);
                try {
                    if (4 == row.get(EpfVideo.MEDIA_TYPE_ID)) {
                        results.put(videoId, episodeExtractor.extract(new ItunesEpfVideoSource(row, extractedLocations.get(String.valueOf(videoId)))));
                    }
                } catch (Exception e) {
                    System.out.println(videoId);
                    throw Throwables.propagate(e);
                }
                return isRunning();
            }

            @Override
            public Map<Integer, Episode> getResult() {
                return results.build();
            }
        });
    }

    private Multimap<Series,Episode> linkEpisodesAndSeries(EpfTable<EpfCollectionVideo> cvTable, final Map<Integer, Series> extractedSeries, final Map<Integer, Episode> extractedEpisodes) throws IOException {
        reportStatus("Linking videos to series...");
        return cvTable.processRows(new EpfTableRowProcessor<EpfCollectionVideo, Multimap<Series,Episode>>() {

            private final HashMultimap<Series,Episode> linkedEpisodes = HashMultimap.create();
            
            @Override
            public boolean process(EpfCollectionVideo row) {
                Series series = extractedSeries.get(row.get(EpfCollectionVideo.COLLECTION_ID));
                if(series != null) {
                    series.withSeriesNumber(row.get(EpfCollectionVideo.VOLUME_NUMBER));
                    Episode ep = extractedEpisodes.get(row.get(EpfCollectionVideo.VIDEO_ID));
                    if(ep != null) {
                        ep.setEpisodeNumber(row.get(EpfCollectionVideo.TRACK_NUMBER));
                        linkedEpisodes.put(series, link(series, ep));
                    }
                }
                return isRunning();
            }

            private Episode link(Series series, Episode ep) {
                ep.setSeries(series);
                ep.setSeriesNumber(series.getSeriesNumber());
                ep.setParentRef(series.getParent());
                return ep;
            }

            @Override
            public Multimap<Series,Episode> getResult() {
                return linkedEpisodes;
            }
            
        });
    }

    private BiMap<Integer, Series> extractSeries(EpfTable<EpfCollection> collTable) throws IOException {
        reportStatus("Extracting series..."); 
        return collTable.processRows(new EpfTableRowProcessor<EpfCollection, BiMap<Integer, Series>>() {

            ImmutableBiMap.Builder<Integer, Series> results = ImmutableBiMap.builder();
            
            @Override
            public boolean process(EpfCollection row) {
                Integer collectionId = row.get(EpfCollection.COLLECTION_ID);

                if (CollectionType.TV_SEASON.equals(row.get(EpfCollection.COLLECTION_TYPE_ID))) {
                    results.put(collectionId, seriesExtractor.extract(row));
                }

                return isRunning();
            }

            @Override
            public BiMap<Integer, Series> getResult() {
                return results.build();
            }
        });
    }

    private BiMap<Integer, Brand> extractBrands(EpfTable<EpfArtist> artistTable) throws IOException {
        reportStatus("Extracting brands..."); 
        return artistTable.processRows(new EpfTableRowProcessor<EpfArtist,BiMap<Integer,Brand>>() {
           
            private final ImmutableBiMap.Builder<Integer, Brand> extractedBrands = ImmutableBiMap.builder();
            
            @Override
            public boolean process(EpfArtist row) {
                if(row.get(EpfArtist.IS_ACTUAL_ARTIST) && ArtistType.TV_SHOW.equals(row.get(EpfArtist.ARTIST_TYPE_ID))) {
                    Brand extractedBrand = extract(row);
                    extractedBrands.put(row.get(EpfArtist.ARTIST_ID), extractedBrand);
                }
                return isRunning();
            }

            private Brand extract(EpfArtist row) {
                Integer rowId = row.get(EpfArtist.ARTIST_ID);
                Brand brand = new Brand(uriForBrand(rowId), curieForBrand(rowId), Publisher.ITUNES);
                brand.setTitle(row.get(EpfArtist.NAME));
                return brand;
            }

            @Override
            public BiMap<Integer,Brand> getResult() {
                return extractedBrands.build();
            }
        });
    }
    

}
