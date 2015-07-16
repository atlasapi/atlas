package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.UpdateProgress;

/**
 * Create a {@link Brand} from a {@link BtVodEntry}. This may either be
 * from brand information contained explicitly in a row, or synthesized
 * from episode data
 *
 * @author tom
 *
 */
public class BtVodBrandWriter implements BtVodDataProcessor<UpdateProgress> {




    static final Pattern HD_PATTERN = Pattern.compile("^(.*)\\-\\sHD");

    private static final Logger log = LoggerFactory.getLogger(BtVodBrandWriter.class);
    private static final boolean CONTINUE = true;
    
    private final Map<String, ParentRef> processedBrands = Maps.newHashMap();
    private final ContentWriter writer;
    private final ContentResolver resolver;
    private final Publisher publisher;
    private final ContentMerger contentMerger;
    private final BtVodContentListener listener;
    private final Set<String> processedRows;
    private final BtVodDescribedFieldsExtractor describedFieldExtractor;
    private final BrandImageExtractor brandImageExtractor;
    private final BrandUriExtractor brandUriExtractor;
    private UpdateProgress progress = UpdateProgress.START;

    public BtVodBrandWriter(
            ContentWriter writer,
            ContentResolver resolver,
            Publisher publisher,
            BtVodContentListener listener,
            Set<String> processedRows,
            BtVodDescribedFieldsExtractor describedFieldExtractor,
            BrandImageExtractor brandImageExtractor,
            BrandUriExtractor brandUriExtractor) {
        this.brandImageExtractor = checkNotNull(brandImageExtractor);
        this.listener = checkNotNull(listener);
        this.writer = checkNotNull(writer);
        this.resolver = checkNotNull(resolver);
        this.publisher = checkNotNull(publisher);
        this.contentMerger = new ContentMerger(MergeStrategy.REPLACE, MergeStrategy.REPLACE, MergeStrategy.REPLACE);
        this.processedRows = checkNotNull(processedRows);
        this.brandUriExtractor = checkNotNull(brandUriExtractor);
        //TODO: Use DescribedFieldsExtractor for all described fields, not just aliases.
        //      Added as a collaborator for Alias extraction, but should be used more 
        //      widely
        this.describedFieldExtractor = checkNotNull(describedFieldExtractor);
    }
    
    @Override
    public boolean process(BtVodEntry row) {
        UpdateProgress thisProgress = UpdateProgress.FAILURE;
        try {
            if ( (!brandUriExtractor.shouldSynthesizeBrand(row))
                    || isBrandAlreadyProcessed(row)
                    || processedRows.contains(getKey(row))) {
                thisProgress = UpdateProgress.SUCCESS;
                return CONTINUE;
            }
            Brand brand = brandFrom(row);
            write(brand);
            listener.onContent(brand, row);
            processedBrands.put(brand.getCanonicalUri(), ParentRef.parentRefFrom(brand));
            thisProgress = UpdateProgress.SUCCESS;
        } catch (Exception e) {
            log.error("Failed to process row " + row.toString(), e);
        }
        finally {
            progress = progress.reduce(thisProgress);
        }
        return CONTINUE;
    }

    private boolean isBrandAlreadyProcessed(BtVodEntry row) {
        Optional<String> optionalUri = brandUriFor(row);
        return optionalUri.isPresent() && processedBrands.containsKey(optionalUri.get());
    }



    private String getKey(BtVodEntry row) {
        String productId = row.getGuid();
        return brandUriExtractor.getSynthesizedKey(row).or(productId);
    }




   
    private void write(Brand extracted) {
        Maybe<Identified> existing = resolver
                .findByCanonicalUris(ImmutableSet.of(extracted.getCanonicalUri()))
                .getFirstValue();
        
        if (existing.hasValue()) {
            Container merged = contentMerger.merge((Brand) existing.requireValue(), 
                                                   extracted);
            writer.createOrUpdate(merged);
        } else {
            writer.createOrUpdate(extracted);
        }
    }

    private Brand brandFrom(BtVodEntry row) {
        Brand brand = new Brand(brandUriFor(row).get(), null, publisher);

        if (brandUriExtractor.canParseBrandFromEpisode(row)) {
            brand.setTitle(brandUriExtractor.brandTitleFromEpisodeTitle(row.getTitle()));
        } else {
            String productId = row.getGuid();
            throw new RuntimeException("Unexpected state - row with product_id: " + productId + " is not a brand nor is possible to parse a brand from it");
        }
        brand.setAliases(describedFieldExtractor.aliasesFrom(row));
        brand.setGenres(describedFieldExtractor.btGenreStringsFrom(row));
        brand.setSpecialization(Specialization.TV);
        brand.setImages(brandImageExtractor.extractImages(row));
        
        VodEntryAndContent vodEntryAndContent = new VodEntryAndContent(row, brand);
        brand.addTopicRefs(describedFieldExtractor.topicsFrom(vodEntryAndContent));
        return brand;
    }
    
    public Optional<String> brandUriFor(BtVodEntry row) {
        return brandUriExtractor.extractBrandUri(row);
    }

    @Override
    public UpdateProgress getResult() {
        return progress;
    }

    public Optional<ParentRef> getBrandRefFor(BtVodEntry row) {
        Optional<String> optionalUri = brandUriFor(row);

        if (!optionalUri.isPresent()) {
            return Optional.absent();
        }

        return Optional.fromNullable(processedBrands.get(optionalUri.get()));
    }

    private String stripHDSuffix(String title) {
        Matcher hdMatcher = HD_PATTERN.matcher(title);
        if (hdMatcher.matches()) {
            return hdMatcher.group(1).trim().replace("- HD ", "");
        }
        return title;
    }
    
}