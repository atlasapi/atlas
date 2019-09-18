package org.atlasapi.remotesite.amazon;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.atlasapi.feeds.tasks.youview.creation.HierarchicalOrdering;
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
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexEntry;
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexStore;
import org.atlasapi.remotesite.bbc.nitro.ModelWithPayload;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

import com.metabroadcast.columbus.telescope.client.EntityType;

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
import static org.atlasapi.remotesite.amazon.AmazonContentExtractor.URI_PREFIX;

public class AmazonContentWritingItemProcessor implements AmazonItemProcessor {

    private static final Ordering<Content> REVERSE_HIERARCHICAL_ORDER = HierarchicalOrdering.create().reverse();

    public static final String GB_AMAZON_ASIN = "gb:amazon:asin"; //this should be kept in sync with a similar field in atlas-feeds
    private static final String UNPUBLISH_NO_PAYLOAD_STRING = "This item lacks payload as it was not seen in this ingest, and consequently it is being unpublished.";

    private static final Predicate<Alias> AMAZON_ALIAS =
            input -> GB_AMAZON_ASIN.equals(input.getNamespace());
    private static final Predicate<String> AMAZON_ALIAS_URL =
            input -> input.startsWith(URI_PREFIX);


    private final Logger log= LoggerFactory.getLogger(AmazonContentWritingItemProcessor.class);
    private final Map<String, ModelWithPayload<? extends Container>>
            seenContainer = Maps.newHashMap();
    private final SetMultimap<String, ModelWithPayload<? extends Content>>
            cached = HashMultimap.create();
    private final Map<String, Brand> topLevelSeries = Maps.newHashMap();
    private final Map<String, Brand> standAloneEpisodes = Maps.newHashMap();
    private final BiMap<String, ModelWithPayload<Content>> seenContent = HashBiMap.create();
    private final Set<String> seriesUris = new HashSet<>();
    private final Set<String> episodeUris = new HashSet<>();
    private final Set<ModelWithPayload<Episode>> episodesWithoutAvailableSeries = new HashSet<>();


    private OwlTelescopeReporter telescope;

    private final ContentExtractor<AmazonItem, Iterable<Content>> extractor;
    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final ContentLister lister;
    private final int missingContentPercentage;
    private final AmazonBrandProcessor brandProcessor;
    private final ContentMerger contentMerger;
    private final AmazonTitleIndexStore amazonTitleIndexStore;

    //Title Cleaning
//    private final Pattern straySymbols = Pattern.compile("^[\\p{Pd}: ]+|[\\p{Pd}: ]+$");//p{Pd}=dashes
//    private final Pattern episodePattern1 = Pattern.compile("(?i)ep[\\.-]*[isode]*[ -]*[\\d]+");//Ep1, Episode 1 etc.
//    private final Pattern episodePattern2 = Pattern.compile("(?i)[s|e]+[\\d]+[ \\.-\\/]*[s|e]+[\\d]+");//S05E01 etc
    private final String titleSeparator = "[ ]*[\\p{Pd}:]+[ ]*"; // any dash or colon.

    public AmazonContentWritingItemProcessor(
            ContentExtractor<AmazonItem, Iterable<Content>> extractor,
            ContentResolver resolver,
            ContentWriter writer,
            ContentLister lister,
            int missingContentPercentage,
            AmazonBrandProcessor brandProcessor,
            AmazonTitleIndexStore amazonTitleIndexStore
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
        this.amazonTitleIndexStore = checkNotNull(amazonTitleIndexStore);
    }

    @Override
    public void prepare(OwlTelescopeReporter telescope) {
        seenContainer.clear();
        cached.clear();
        topLevelSeries.clear();
        standAloneEpisodes.clear();
        seenContent.clear();
        seriesUris.clear();
        episodeUris.clear();
        episodesWithoutAvailableSeries.clear();

        this.telescope = telescope;
    }
    
    @Override
    public void process(AmazonItem item) {
        for (Content content : extract(item)) {
            ModelWithPayload<Content> contentWithPayload = new ModelWithPayload<>(content, item);
            if (shouldDiscard(contentWithPayload)) {
                continue;
            }

            seenContent.put(content.getCanonicalUri(), contentWithPayload);

            //Keep a note of all series and episodes
            if (content instanceof Series) {
                seriesUris.add(content.getCanonicalUri());
            } else if (content instanceof Episode) {
                episodeUris.add(content.getCanonicalUri());
            }
        }
    }

    private boolean shouldDiscard(ModelWithPayload<Content> contentWithPayload) {
        if (contentWithPayload.getModel() instanceof Episode) {
            Episode episode = contentWithPayload.asModelType(Episode.class).getModel();
            Integer episodeNumber = episode.getEpisodeNumber();
            // YV has requested we do not sent trailers, and we don't want to keep trailers either.
            // According to amazon trailers are identified with episodeNumbers 000 or 101.
            if (episodeNumber == 0 || episodeNumber == 101) {
                telescope.reportFailedEvent(
                        "Episode was discarded because it was a trailer (index 0 or 101)",
                        EntityType.EPISODE,
                        contentWithPayload.getPayload()
                );
                return true;
            }
        }
        if (contentWithPayload.getModel() instanceof Item) {
            Item item = contentWithPayload.asModelType(Item.class).getModel();
            //We also discard Clips. ECOTEST-429. CPINC-1223 removed the duration restriction.
            if (item.getTitle() != null) {
                if (!item.getVersions().isEmpty()) {
                    if (item.getTitle().toLowerCase().startsWith("clip:")) {
                        telescope.reportFailedEvent(
                                "Content was discarded because it was a clip",
                                contentWithPayload.getModel(),
                                contentWithPayload.getPayload()
                        );
                        return true;
                    }
                } else if (item.getTitle().toLowerCase().startsWith("clip:")) {
                    log.warn("AMAZ-CLIP: found clip title but with no versions");
                }
            }

        }
        return false;
    }

    @Override
    public void finish() {
        titleCleaning();
//        assignImagesToBrands();

        processSeenContent(telescope);
        processTopLevelSeries(telescope);
        processStandAloneEpisodes(telescope);

        if (!cached.isEmpty()) {
            log.warn(
                    "{} pieces of amazon content have been referenced but not seen",
                    cached.keys().size()
            );
            telescope.reportFailedEvent(
                    "The following content has been referenced, but was not seen itself in the feed",
                    cached.asMap()
            );
        }

        createTitleIndex();

        seenContainer.clear();
        cached.clear();
        topLevelSeries.clear();
        standAloneEpisodes.clear();
        
        checkForDeletedContent(telescope);

        seenContent.clear();
        seriesUris.clear();
        episodeUris.clear();
        episodesWithoutAvailableSeries.clear();

        telescope = null;
    }

    private void assignImagesToBrands() {
        //As Brands are synthesized we want them to have the same image. We will pick the
        //image from season 1.
        // Because brands don't contain series ref at this point we'll go the other way around.
        // We'll loop the series and assign images to their parents.

        //try all series number in order, so lower numbers are preferred
        for (int seriesNo = 1; seriesNo < 100 && !seriesUris.isEmpty(); seriesNo++) {
            Iterator<String> iterator = seriesUris.iterator();
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

    private void createTitleIndex() {
        //Title -> Set<Content> (stores object references instead of just uri to reduce memory use)
        SetMultimap<String, Content> titleIndex = HashMultimap.create();
        for (ModelWithPayload<Content> content : seenContent.values()) {
            if(isTopLevelContent(content.getModel())) {
                titleIndex.put(content.getModel().getTitle(), content.getModel());
            }
        }
        Set<String> titles = titleIndex.keySet();
        log.info("Creating title index for {} unique titles", titles.size());
        for (String title : titles) {
            Set<String> uris = titleIndex.get(title).stream()
                    .map(Content::getCanonicalUri)
                    .collect(Collectors.toSet());
            AmazonTitleIndexEntry indexEntry = new AmazonTitleIndexEntry(title, uris);
            amazonTitleIndexStore.createOrUpdateIndex(indexEntry);
        }
        log.info("Title index creation finished");
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

    private void titleCleaning() {
        // YV presents content as brand - series - episode. Thus, we would like not to repeat
        // the brand or series title in the episode title. It was agreed that metabroadcast
        // would perform a simple cleaning task of the below format.
        // (in case you are wondering, series are replaced by their numbers on output).
        // https://jira-ngyv.youview.co.uk/browse/ECOTEST-268
        for (String uri : episodeUris) {
            Episode episode = (Episode) seenContent.get(uri).getModel();
            Series series = getSeries(episode);
            Brand brand = getBrand(series);

            String title = episode.getTitle();
            if (series != null && series.getTitle() != null) {
                title = removeTitle(title, series.getTitle());
            }
            if (brand != null && brand.getTitle() != null) {
                title = removeTitle(title, brand.getTitle());
            }

            //if nothing was left, try to construct a title from the episode Number
            if (title.isEmpty()) {
                if (episode.getEpisodeNumber() != null && episode.getEpisodeNumber() != 0) {
                    title = "Episode " + episode.getEpisodeNumber();
                } else { //fallback to what it was and do nothing
                    title = episode.getTitle();
                }
            }

            episode.setTitle(title);
        }
    }

    private String removeTitle(String goodTitle, String badTitle) {
        //YV asked to not do that until they get a chance to review the content.
        //        if(goodTitle.equals(badTitle)){
        //            return "";
        //        }
        Pattern titleWithSeparator =
                Pattern.compile("^" + Pattern.quote(badTitle.trim()) + titleSeparator, Pattern.CASE_INSENSITIVE);

        String cleanTitle = titleWithSeparator.matcher(goodTitle).replaceAll("").trim();
        return cleanTitle;
    }

    @Nullable
    private Brand getBrand(Series series) {
        if (series != null
        && series.getParent() != null
        && series.getParent().getUri() != null) {
            ModelWithPayload<Content> brand =
                    seenContent.get(series.getParent().getUri());
            if (brand != null) {
                return (Brand) brand.getModel();
            }
        }
        return null;
    }

    @Nullable
    private Series getSeries(Episode episode) {
        if (episode.getSeriesRef() != null
            && episode.getSeriesRef().getUri() != null) {
            ModelWithPayload<Content> series =
                    seenContent.get(episode.getSeriesRef().getUri());
            if (series != null) {
                return (Series) series.getModel();
            }
        }
        return null;
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
        //and because the merge doesnt really merge, add the aliases
        merged.addAliases(contentWithPayload.getModel().getAliases());
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
        Iterator<Content> allAmazonContent = resolveAllAmazonContent();
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

        unpublishUnseenContent(telescope, notSeen);
        unpublishParentlessChildren(telescope, episodesWithoutAvailableSeries);
    }

    /**
     * This will attempt to unpublish parentless Children. The caveat is that we can't save
     * children if they don't have parents, but their parents might have gone away from the
     * feed, in which case we need to unpublish existing things, and not do anything for
     * any new ingests.
     */
    private void unpublishParentlessChildren(OwlTelescopeReporter telescope,
            Set<ModelWithPayload<Episode>> contentSet) {
        int unpublished = 0;
        int notSaved = 0;
        for (ModelWithPayload<Episode> contentWithPayload : contentSet) {
            Episode content = contentWithPayload.getModel();
            content.setActivelyPublished(false);

            //instead of resolving from db and then trying to write to the db which will
            //resolve again, we'll abuse the exception thrown if the thing is parentless.
            //If we can write it, it means it already had saved parents, and will be
            //unpublished. If throws an ISE then it probably did not have parents and
            //couldn't be saved.
            try {
                writer.createOrUpdate(content);
                telescope.reportSuccessfulEventWithWarning(
                        content.getId(),
                        content.getAliases(),
                        EntityType.EPISODE,
                        "The series of this episode have been unpublished, and so this episode is being unpublished as well.",
                        contentWithPayload.getPayload()
                );
                unpublished++;
            } catch (IllegalStateException e){
                telescope.reportFailedEvent(
                        "The series of this episode are missing from the feed, and consequently it cannot be ingested.",
                        EntityType.EPISODE,
                        contentWithPayload.getPayload()
                );
                notSaved++;
            }

        }
        log.info("{} episodes were unpublished because their series disappeared from the feed.", unpublished);
        log.info("{} episodes were not saved at all because they had no series in the feed.", notSaved);
    }

    private void unpublishUnseenContent(OwlTelescopeReporter telescope, Set<Content> contentSet) {
        for (Content notSeenContent : contentSet) {
            notSeenContent.setActivelyPublished(false);
            if (notSeenContent instanceof Item) {
                writer.createOrUpdate((Item) notSeenContent);
                telescope.reportSuccessfulEvent(
                        notSeenContent.getId(),
                        notSeenContent.getAliases(),
                        notSeenContent,
                        notSeenContent.getCanonicalUri(),
                        AmazonContentWritingItemProcessor.UNPUBLISH_NO_PAYLOAD_STRING);

            } else if (notSeenContent instanceof Container) {
                writer.createOrUpdate((Container) notSeenContent);
                telescope.reportSuccessfulEvent(
                        notSeenContent.getId(),
                        notSeenContent.getAliases(),
                        notSeenContent,
                        notSeenContent.getCanonicalUri(),
                        AmazonContentWritingItemProcessor.UNPUBLISH_NO_PAYLOAD_STRING);

            } else {
                log.error("Amazon content {} not an Item or a Container,"
                          + " and thus cannot be unpublished",
                        notSeenContent.getCanonicalUri() );
            }
        }
    }

    private Iterator<Content> resolveAllAmazonContent() {
        ContentListingCriteria criteria = ContentListingCriteria
            .defaultCriteria()
            .forPublisher(Publisher.AMAZON_UNBOX)
            .build();
        
        return lister.listContent(criteria);
    }
    
    public Iterable<Content> extract(AmazonItem item) {
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
                episodesWithoutAvailableSeries.add(episode);
                return;
            }
        }
        
        String seriesUri = episode.getModel().getSeriesRef() != null
                           ? episode.getModel().getSeriesRef().getUri()
                           : null;
        if (seriesUri != null ) {
            if (!seenContainer.containsKey(seriesUri)) {
                episodesWithoutAvailableSeries.add(episode);
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

    public static boolean isTopLevelContent(Content content) {
        if(content instanceof Item) {
            if (content instanceof Episode) {
                return false;
            }
            if (((Item) content).getContainer() != null) {
                return false;
            }
        } else if(content instanceof Container) {
            if (content instanceof Series) {
                if (((Series) content).getParent() != null) {
                    return false;
                }
            }
        }
        return true;
    }
}
