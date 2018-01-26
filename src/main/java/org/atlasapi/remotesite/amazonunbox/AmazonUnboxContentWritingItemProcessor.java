package org.atlasapi.remotesite.amazonunbox;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;
import org.atlasapi.remotesite.bbc.nitro.ModelWithPayload;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

import com.metabroadcast.common.base.Maybe;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.remotesite.amazonunbox.AmazonUnboxContentExtractor.URI_PREFIX;

public class AmazonUnboxContentWritingItemProcessor implements AmazonUnboxItemProcessor {

    private static final Ordering<Content> REVERSE_HIERARCHICAL_ORDER = new Ordering<Content>() {
        @Override
        public int compare(Content left, Content right) {
            if (left instanceof Item) {
                if (right instanceof Item) {
                    return 0;
                } else {
                    return -1;
                }
            } else if (left instanceof Series) {
                if (right instanceof Item) {
                    return 1;
                } else if (right instanceof Series) {
                    return 0;
                } else {
                    return -1;
                }
            } else {
                if (right instanceof Brand) {
                    return 0;
                } else {
                    return 1;
                }
            }
        }
    };

    public static final String GB_AMAZON_ASIN = "gb:amazon:asin";
    private static final String UNPUBLISH_NO_PAYLOAD_STRING = "This item lacks payload as it was not seen in the this ingest, and consequently it is being unpublished.";

    private static final Predicate<Alias> AMAZON_ALIAS =
            input -> GB_AMAZON_ASIN.equals(input.getNamespace());
    private static final Predicate<String> AMAZON_ALIAS_URL =
            input -> input.startsWith(URI_PREFIX);


    private final Logger log = LoggerFactory.getLogger(
            AmazonUnboxContentWritingItemProcessor.class
    );
    private final Map<String, ModelWithPayload<? extends Container>>
            seenContainer = Maps.newHashMap();
    private final SetMultimap<String, ModelWithPayload<? extends Content>>
            cached = HashMultimap.create();
    private final Map<String, Brand> topLevelSeries = Maps.newHashMap();
    private final Map<String, Brand> standAloneEpisodes = Maps.newHashMap();
    private final BiMap<String, ModelWithPayload<Content>> seenContent = HashBiMap.create();
    private final Set<String> seriesForImageExtraction = new HashSet<>();

    private final ContentExtractor<AmazonUnboxItem, Iterable<Content>> extractor;
    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final ContentLister lister;
    private final int missingContentPercentage;
    private final AmazonUnboxBrandProcessor brandProcessor;
    private final ContentMerger contentMerger;

    public AmazonUnboxContentWritingItemProcessor(
            ContentExtractor<AmazonUnboxItem, Iterable<Content>> extractor,
            ContentResolver resolver,
            ContentWriter writer,
            ContentLister lister,
            int missingContentPercentage,
            AmazonUnboxBrandProcessor brandProcessor
    ) {
        this.extractor = checkNotNull(extractor);
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);
        this.lister = checkNotNull(lister);
        this.missingContentPercentage = missingContentPercentage;
        this.brandProcessor = checkNotNull(brandProcessor);
        this.contentMerger = new ContentMerger(
                MergeStrategy.REPLACE,
                MergeStrategy.REPLACE,
                MergeStrategy.REPLACE
        );
    }

    @Override
    public void prepare() {
        seenContainer.clear();
        cached.clear();
        topLevelSeries.clear();
        standAloneEpisodes.clear();
        seenContent.clear();
        seriesForImageExtraction.clear();
    }
    
    @Override
    public void process(AmazonUnboxItem item) {
        for (Content content : extract(item)) {
            ModelWithPayload<Content> contentWithPayload = new ModelWithPayload<>(content, item);
            seenContent.put(content.getCanonicalUri(), contentWithPayload);

            //As Brands are synthesized we want them to have the same image. We will pick the
            //image from season 1, but for this to become available we first need to ingest
            //everything. So we'll keep the note of all series.
            if( content instanceof Series){
                seriesForImageExtraction.add(content.getCanonicalUri());
            }
        }
    }

    @Override
    public void finish(OwlTelescopeReporter telescope) {
        assignImagesToBrands();

        processSeenContent(telescope);
        processTopLevelSeries(telescope);
        processStandAloneEpisodes(telescope);

        if (!cached.isEmpty()) {
            log.warn("{} pieces of amazon content have been extracted but not written", cached.values().size());
            for (Entry<String, Collection<ModelWithPayload<? extends Content>>>
                    mapping : cached.asMap().entrySet()) {

                for (ModelWithPayload<? extends Content> contentWithPayload : mapping.getValue()) {
                    telescope.reportFailedEvent(
                            contentWithPayload.getModel().getId(),
                            "Content has been extracted but not written",
                            contentWithPayload.getModel(),
                            contentWithPayload.getPayload()
                            );
                    log.warn("Not written: {} - {} ", mapping.getKey(), contentWithPayload.getModel() );
                }
            }
        }

        seenContainer.clear();
        cached.clear();
        topLevelSeries.clear();
        standAloneEpisodes.clear();
        
        checkForDeletedContent(telescope);

        seenContent.clear();
    }

    private void assignImagesToBrands() {
        // Because brands don't contain series ref at this point we'll go the other way around.
        // We'll loop the series and assign images to their parents.

        //try all series number in order, so lower numbers are preferred
        for (int seriesNo = 1; seriesNo < 100 && !seriesForImageExtraction.isEmpty(); seriesNo++) {
            Iterator<String> iterator = seriesForImageExtraction.iterator();
            while (iterator.hasNext()) {
                String uri = iterator.next();
                Series series = (Series) seenContent.get(uri).getModel();
                if (series.getSeriesNumber() == null || series.getSeriesNumber() == 0) {
                    iterator.remove();
                }
                if (series.getSeriesNumber() == seriesNo) {
                    iterator.remove();
                    assignImageToParent(series);
                }
            }
        }
    }

    private void assignImageToParent(Series series) {
        if (series.getImage() != null && !series.getImage().equals("")) {
            String parentUri = series.getParent().getUri();
            if (parentUri != null) {
                ModelWithPayload<Content> parent = seenContent.get(parentUri);
                if (parent != null) {
                    Content model = parent.getModel();
                    if (model.getImage() == null || model.getImage().equals("")) {
                        model.setImage(series.getImage());
                    }
                }
            }
        }
    }

    private void processSeenContent(OwlTelescopeReporter telescope) {
        List<ModelWithPayload<Content>> brands = new ArrayList<>();
        List<ModelWithPayload<Content>> series = new ArrayList<>();
        List<ModelWithPayload<Content>> notContainers = new ArrayList<>();

        for (ModelWithPayload<Content> content : seenContent.values()){
            if (content.getModel() instanceof Brand){
                brands.add(content);
            } else if (content.getModel() instanceof Series){
                series.add(content);
            } else {
                notContainers.add(content);
            }
        }

        //Process Brands first, Series next and everything else last.
        brands.forEach(c -> checkAndWriteSeenContent(c, telescope));
        series.forEach(c -> checkAndWriteSeenContent(c, telescope));
        notContainers.forEach(c -> checkAndWriteSeenContent(c, telescope));
    }

    private void checkAndWriteSeenContent(
            ModelWithPayload<Content> content,
            OwlTelescopeReporter telescope) {

        Maybe<Identified> existing = resolve(content.getModel().getCanonicalUri());
        if (existing.isNothing()) {
            write(content, telescope);
        } else {
            Identified identified = existing.requireValue();
            if (content.getModel() instanceof Item) {
                write(mergeItemsWithPayload(
                            ContentMerger.asItem(identified),
                            content.asModelType(Item.class)),
                        telescope);
            } else if (content.getModel() instanceof Container) {
                write(mergeContainersWithPayload(
                            ContentMerger.asContainer(identified),
                            content.asModelType(Container.class)),
                        telescope);
            }
        }
    }

    private ModelWithPayload<? extends Item> mergeItemsWithPayload(
            Item existingContent,
            ModelWithPayload<? extends Item> contentWithPayload
    ) {
            Item merged = contentMerger.merge(existingContent, contentWithPayload.getModel());
            return new ModelWithPayload<>(merged, contentWithPayload.getPayload());
    }


    private ModelWithPayload<? extends Container> mergeContainersWithPayload(
            Container existingContent,
            ModelWithPayload<? extends Container> contentWithPayload
    ) {
        Container merged = contentMerger.merge(existingContent, contentWithPayload.getModel());
        return new ModelWithPayload<>(merged, contentWithPayload.getPayload());
    }

    private void processStandAloneEpisodes(OwlTelescopeReporter telescope) {
        for (Entry<String, Brand> entry : standAloneEpisodes.entrySet()) {

            List<ModelWithPayload<Item>> cachedItems =
                    cached.get(entry.getKey()).stream()
                            .filter(c -> c.getModel() instanceof Item)
                            .map(c -> c.asModelType(Item.class))
                            .collect(Collectors.toList());

            ModelWithPayload<Item> item = Iterables.getOnlyElement(cachedItems);

            cached.removeAll(entry.getKey());
            
            if (item.getModel() instanceof Episode) {
                Episode episode = (Episode) item.getModel();
                episode.setSeriesRef(null);
                episode.setSeriesNumber(null);
            }
            item.getModel().setParentRef(null);
            write(item, telescope);
        }
    }

    private void processTopLevelSeries(OwlTelescopeReporter telescope) {
        for (Entry<String, Brand> entry : topLevelSeries.entrySet()) {

            List<ModelWithPayload<Series>> cachedSeries =
                    cached.get(entry.getKey()).stream()
                            .filter(c -> c.getModel() instanceof Series)
                            .map(c -> c.asModelType(Series.class))
                            .collect(Collectors.toList());

            ModelWithPayload<Series> series = Iterables.getOnlyElement(cachedSeries);

            cached.remove(entry.getKey(), series);
            
            series.getModel().setParentRef(null);
            String seriesUri = series.getModel().getCanonicalUri();
            // TODO will this update the refs?
            for (ModelWithPayload<? extends Content> child : cached.get(seriesUri)) {
                Episode episode = (Episode) child.getModel();
                episode.setParentRef(ParentRef.parentRefFrom(series.getModel()));
                episode.setSeriesRef(null);
                episode.setSeriesNumber(null);
                //write(episode);
            }
            // this will write those items cached against the series
            write(series, telescope);
            
            for (ModelWithPayload<? extends Content> child : cached.removeAll(entry.getKey())) {
                if (child.getModel() instanceof Item) {
                    ModelWithPayload<Item> item = child.asModelType(Item.class);
                    if (item.getModel() instanceof Episode) {
                        Episode episode = (Episode) child.getModel();
                        episode.setSeriesRef(null);
                        episode.setSeriesNumber(null);
                    } 
                    item.getModel().setParentRef(ParentRef.parentRefFrom(series.getModel()));
                    write(item, telescope);
                }
            }
        }
    }

    private void checkForDeletedContent(OwlTelescopeReporter telescope) {
        //get all amazon content from the db. See if there are any items we have not seen in the current ingest.
        Set<Content> notSeen = new TreeSet<>(REVERSE_HIERARCHICAL_ORDER);
        Iterator<Content> allAmazonContent = resolveAllAmazonUnboxContent();
        int allAmazonContentSize = 1; //prevent division by zero later on.
        while(allAmazonContent.hasNext()){
            allAmazonContentSize++;
            Content examinedContent = allAmazonContent.next();
            if(!seenContent.containsKey(examinedContent.getCanonicalUri())){
                notSeen.add(examinedContent);
            }
        }

        //make sure we won't disable all amazon content due to a corrupted file
        float missingPercentage = ( notSeen.size() / (float) allAmazonContentSize) * 100; //cast to convert to floating point calculations.
        if (missingPercentage > (float) missingContentPercentage) {
            telescope.reportFailedEvent(missingPercentage +
                                        "% of all Amazon content is missing from the ingest file. "
                                        + "As this is abnormal, nothing was unpublished. "
                                        + "It is possible the file is truncated.");
            throw new IllegalStateException("File failed to update " +
                            missingPercentage +
                            "% of all Amazon content. File may be truncated.");
        }

        unpublishContent(telescope, notSeen);
    }

    private void unpublishContent(OwlTelescopeReporter telescope, Set<Content> contentSet) {
        for (Content notSeenContent : contentSet) {
            notSeenContent.setActivelyPublished(false);
            if (notSeenContent instanceof Item) {
                writer.createOrUpdate((Item) notSeenContent);
                telescope.reportSuccessfulEvent(
                        notSeenContent.getId(),
                        notSeenContent.getAliases(),
                        notSeenContent,
                        UNPUBLISH_NO_PAYLOAD_STRING);

            } else if (notSeenContent instanceof Container) {
                writer.createOrUpdate((Container) notSeenContent);
                telescope.reportSuccessfulEvent(
                        notSeenContent.getId(),
                        notSeenContent.getAliases(),
                        notSeenContent,
                        UNPUBLISH_NO_PAYLOAD_STRING);

            } else {
                telescope.reportFailedEvent(
                        "Amazon Unbox content with uri "
                        + notSeenContent.getCanonicalUri()
                        + " not an Item or a Container, and thus"
                        + "cannot be unpublished."
                );

                log.error("Amazon Unbox content with uri {} not an Item or a Container,"
                          + " and thus cannot unpublished",
                        notSeenContent.getCanonicalUri() );
            }
        }
    }

    private Iterator<Content> resolveAllAmazonUnboxContent() {
        ContentListingCriteria criteria = ContentListingCriteria
            .defaultCriteria()
            .forPublisher(Publisher.AMAZON_UNBOX)
            .build();
        
        return lister.listContent(criteria);
    }
    
    public Iterable<Content> extract(AmazonUnboxItem item) {
        return extractor.extract(item);
    }
    
    private Maybe<Identified> resolve(String uri) {
        ImmutableSet<String> uris = ImmutableSet.of(uri);
        return resolver.findByCanonicalUris(uris).get(uri);
    }
    
    private void write(ModelWithPayload<? extends Content> content, OwlTelescopeReporter telescope) {
        if (content.getModel() instanceof Container) {
            if (content.getModel() instanceof Series) {
                cacheOrWriteSeriesAndSubContents(content.asModelType(Series.class), telescope);
            } else if (content.getModel() instanceof Brand) {
                cacheOrWriteBrandAndCachedSubContents(content.asModelType(Brand.class), telescope);
            } else {
                writer.createOrUpdate(content.asModelType(Container.class).getModel());
                reportContentWithPayloadToTelescope(content, telescope);

            }
        } else if (content.getModel() instanceof Item) {
            if (content.getModel() instanceof Episode) {
                cacheOrWriteEpisode(content.asModelType(Episode.class), telescope);
            } else {
                cacheOrWriteItem(content.asModelType(Content.class), telescope);
            }
        }
    }

    private void cacheOrWriteBrandAndCachedSubContents(
            ModelWithPayload<Brand> brand,
            OwlTelescopeReporter telescope) {
        String brandUri = brand.getModel().getCanonicalUri();
        BrandType brandType = brandProcessor.getBrandType(brandUri);
        if (BrandType.TOP_LEVEL_SERIES.equals(brandType)) {
            log.trace("Caching top-level series {}", brandUri);
            topLevelSeries.put(brandUri, brand.getModel());
        } else if (BrandType.STAND_ALONE_EPISODE.equals(brandType)) {
            log.trace("Caching stand-alone episode {}", brandUri);
            standAloneEpisodes.put(brandUri, brand.getModel());
        } else {
            writeBrand(brand, telescope);
        }
    }
    
    private void writeBrand(ModelWithPayload<Brand> brand, OwlTelescopeReporter telescope) {
        writer.createOrUpdate(brand.getModel());
        reportContentWithPayloadToTelescope(brand, telescope);

        String brandUri = brand.getModel().getCanonicalUri();
        seenContainer.put(brandUri, brand);
        for (ModelWithPayload<? extends Content> subContent : cached.removeAll(brandUri)) {
            write(subContent, telescope);
        }
    }

    private void cacheOrWriteSeriesAndSubContents(
            ModelWithPayload<Series> series,
            OwlTelescopeReporter telescope) {
        ParentRef parent = series.getModel().getParent();
        if (parent != null) {
            ModelWithPayload<Content> brand = seenContent.get(parent.getUri());
            if (brand != null && brand.getModel() instanceof Brand) {
                series.getModel().setParent(brand.asModelType(Brand.class).getModel());
            }
            String brandUri = series.getModel().getParent().getUri();
            if (!seenContainer.containsKey(brandUri)) {
                cached.put(brandUri, series);
                return;
            }
        }
        writeSeries(series, telescope);
    }

    public void writeSeries(ModelWithPayload<Series> series, OwlTelescopeReporter telescope) {
        String seriesUri = series.getModel().getCanonicalUri();
        writer.createOrUpdate(series.getModel());
        reportContentWithPayloadToTelescope(series, telescope);

        seenContainer.put(seriesUri, series);
        for (ModelWithPayload<? extends Content> episode : cached.removeAll(seriesUri)) {
            write(episode, telescope);
        }
    }

    private void cacheOrWriteItem(
            ModelWithPayload<Content> content,
            OwlTelescopeReporter telescope) {
        ModelWithPayload<Item> item = content.asModelType(Item.class);
        ParentRef parent = item.getModel().getContainer();
        if (parent != null && !seenContainer.containsKey(parent.getUri())) {
            cached.put(parent.getUri(), item);
        } else {
            writer.createOrUpdate(item.getModel());
            reportContentWithPayloadToTelescope(item, telescope);
        }
    }

    private void cacheOrWriteEpisode(
            ModelWithPayload<Episode> episode,
            OwlTelescopeReporter telescope) {
        ParentRef parent = episode.getModel().getContainer();

        if (parent != null) {
            ModelWithPayload<Content> brand = seenContent.get(parent.getUri());
            String brandUri = brand.getModel().getCanonicalUri();
            if (!seenContainer.containsKey(brandUri)) {
                cached.put(brandUri, episode);
                return;
            }
        }
        
        String seriesUri = episode.getModel().getSeriesRef() != null
                           ? episode.getModel().getSeriesRef().getUri()
                           : null;
        if (seriesUri != null ) {
            if (!seenContainer.containsKey(seriesUri)) {
                cached.put(seriesUri, episode);
                return;
            }
            ModelWithPayload<Content> series = seenContent.get(seriesUri);
            if(series.getModel() instanceof Series) {
                episode.getModel().setSeriesNumber(series.asModelType(Series.class).getModel().getSeriesNumber());
            }
        }

        writer.createOrUpdate(episode.getModel());
        reportContentWithPayloadToTelescope(episode, telescope);
    }

    private void reportContentWithPayloadToTelescope(
            ModelWithPayload<? extends Content> content,
            OwlTelescopeReporter telescope)
    {
        if(content.getModel().getId() != null){
            telescope.reportSuccessfulEvent(
                    content.getModel().getId(),
                    content.getModel().getAliases(),
                    content.getModel(),
                    content.getPayload()
            );
        } else {
            telescope.reportFailedEvent(
                    "Atlas did not return an id after attempting to create or update this content",
                    content,
                    content.getPayload()
            );
        }
    }
}
