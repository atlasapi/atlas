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

    private static final List<Pattern> BRAND_TITLE_FROM_EPISODE_PATTERNS = ImmutableList.of(
            Pattern.compile("^(.*):.*S[0-9]+.*S[0-9]+\\-E.*"),
            Pattern.compile("^(.*).*S[0-9]+\\-E.*")
    );
    private static final Pattern BRAND_TITLE_FROM_SERIES_PATTERN = Pattern.compile("^(.*) Series [0-9]+");
    private static final String HELP_TYPE = "help";

    private static final Logger log = LoggerFactory.getLogger(BtVodBrandWriter.class);
    private static final boolean CONTINUE = true;
    
    private final Map<String, ParentRef> processedBrands = Maps.newHashMap();
    private final ContentWriter writer;
    private final ContentResolver resolver;
    private final Publisher publisher;
    private final String uriPrefix;
    private final ContentMerger contentMerger;
    private final BtVodContentListener listener;
    private final Set<String> processedRows;
    private final BtVodDescribedFieldsExtractor describedFieldsExtractor;
    private UpdateProgress progress = UpdateProgress.START;
    
    public BtVodBrandWriter(ContentWriter writer, ContentResolver resolver,
            Publisher publisher, String uriPrefix, BtVodContentListener listener, 
            BtVodDescribedFieldsExtractor describedFieldsExtractor, Set<String> processedRows) {
        this.describedFieldsExtractor = checkNotNull(describedFieldsExtractor);
        this.listener = checkNotNull(listener);
        this.writer = checkNotNull(writer);
        this.resolver = checkNotNull(resolver);
        this.publisher = checkNotNull(publisher);
        this.uriPrefix = checkNotNull(uriPrefix);
        this.contentMerger = new ContentMerger(MergeStrategy.REPLACE, MergeStrategy.KEEP, MergeStrategy.REPLACE);
        this.processedRows = checkNotNull(processedRows);
    }
    
    @Override
    public boolean process(BtVodEntry row) {
        UpdateProgress thisProgress = UpdateProgress.FAILURE;
        try {
            if ( (!shouldSynthesizeBrand(row))
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
        Optional<String> optionalUri = uriFor(row);
        return optionalUri.isPresent() && processedBrands.containsKey(optionalUri.get());
    }

    public Optional<String> getSynthesizedKey(BtVodEntry row) {
        String title = row.getTitle();

        if (canParseBrandFromEpisode(row)) {
            return Optional.of(Sanitizer.sanitize(brandTitleFromEpisodeTitle(title)));
        }

        return Optional.absent();
    }

    private String getKey(BtVodEntry row) {
        String productId = row.getGuid();
        return getSynthesizedKey(row).or(productId);
    }

    private String brandTitleFromEpisodeTitle(String title) {
        for (Pattern brandPattern : BRAND_TITLE_FROM_EPISODE_PATTERNS) {
            Matcher matcher = brandPattern.matcher(title);
            if (matcher.matches()) {
                return matcher.group(1);
            }

        }
        return null;
    }

    private String brandTitleFromSeriesTitle(String title) {
        Matcher matcher = BRAND_TITLE_FROM_SERIES_PATTERN.matcher(title);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return null;
    }

    private boolean shouldSynthesizeBrand(BtVodEntry row) {
        return !HELP_TYPE.equals(row.getBtproduct$productType()) && canParseBrandFromEpisode(row);
    }

    private boolean canParseBrandFromEpisode(BtVodEntry row) {
        return isTitleSyntesizableFromEpisode(row.getTitle());
    }

    private boolean isTitleSynthesizableFromSeries(String title) {
        Matcher matcher = BRAND_TITLE_FROM_SERIES_PATTERN.matcher(title);
        return matcher.matches();
    }

    private boolean isTitleSyntesizableFromEpisode(String title) {
        for (Pattern brandPattern : BRAND_TITLE_FROM_EPISODE_PATTERNS) {
            if (brandPattern.matcher(title).matches()) {
                return true;
            }

        }
        return false;
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
        Brand brand = new Brand(uriFor(row).get(), null, publisher);

        if (canParseBrandFromEpisode(row)) {
            brand.setTitle(brandTitleFromEpisodeTitle(row.getTitle()));
        } else {
            String productId = row.getGuid();
            throw new RuntimeException("Unexpected state - row with product_id: " + productId + " is not a brand nor is possible to parse a brand from it");
        }

        describedFieldsExtractor.setDescribedFieldsFrom(row, brand);
        return brand;
    }
    
    public Optional<String> uriFor(BtVodEntry row) {
        if (!shouldSynthesizeBrand(row)) {
            return Optional.absent();
        } else {
            return Optional.of(uriPrefix + "synthesized/brands/" + ensureSynthesizedKey(row));
        }
    }

    private String ensureSynthesizedKey(BtVodEntry row) {
        Optional<String> synthesizedKey = getSynthesizedKey(row);

        if (!synthesizedKey.isPresent()) {
            String productId = row.getGuid();
            throw new RuntimeException("Not able to generate a synthesized key for a synthesizable brand from row: [PRODUCT_ID="+productId+"]");
        }

        return synthesizedKey.get();
    }

    @Override
    public UpdateProgress getResult() {
        return progress;
    }

    public Optional<ParentRef> getBrandRefFor(BtVodEntry row) {
        Optional<String> optionalUri = uriFor(row);

        if (!optionalUri.isPresent()) {
            return Optional.absent();
        }

        return Optional.fromNullable(processedBrands.get(optionalUri.get()));
    }
    
}