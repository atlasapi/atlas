package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;
import org.atlasapi.remotesite.btvod.BtVodData.BtVodDataRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.UpdateProgress;

/**
 * Create a {@link Brand} from a {@link BtVodDataRow}. This may either be
 * from brand information contained explicitly in a row, or synthesized 
 * from episode data
 * 
 * @author tom
 *
 */
public class BtVodBrandWriter implements BtVodDataProcessor<UpdateProgress> {

    private static final Pattern BRAND_TITLE_FROM_EPISODE_PATTERN = Pattern.compile("^(.*)\\ S[0-9]+\\-E.*");
    private static final Pattern BRAND_TITLE_FROM_SERIES_PATTERN = Pattern.compile("^(.*) Series [0-9]+");

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
    public boolean process(BtVodDataRow row) {
        UpdateProgress thisProgress = UpdateProgress.FAILURE;
        try {
            /**
             * TODO: In the current data file the column BrandIA_ID is always empty
             * Should we keep checking it or just ignore it?
            */
            if ( (!isBrand(row) && !shouldSynthesizeBrand(row))
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

    private boolean isBrandAlreadyProcessed(BtVodDataRow row) {
        Optional<String> optionalUri = uriFor(row);
        return optionalUri.isPresent() && processedBrands.containsKey(optionalUri.get());
    }

    private Optional<String> getSynthesizedKey(BtVodDataRow row) {
        String title = row.getColumnValue(BtVodFileColumn.PRODUCT_TITLE);

        if (canParseBrandFromSeries(row)) {
            return Optional.of(Sanitizer.sanitize(brandTitleFromSeriesTitle(title)));
        }

        if (canParseBrandFromEpisode(row)) {
            return Optional.of(Sanitizer.sanitize(brandTitleFromEpisodeTitle(title)));
        }

        return Optional.absent();
    }

    private String getKey(BtVodDataRow row) {
        String productId = row.getColumnValue(BtVodFileColumn.PRODUCT_ID);
        return getSynthesizedKey(row).or(productId);
    }

    private String brandTitleFromEpisodeTitle(String title) {
        Matcher matcher = BRAND_TITLE_FROM_EPISODE_PATTERN.matcher(title);
        if (matcher.matches()) {
            return matcher.group(1);
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

    private boolean shouldSynthesizeBrand(BtVodDataRow row) {
        return !isBrand(row) && (canParseBrandFromEpisode(row) || canParseBrandFromSeries(row));
    }

    private boolean isBrand(BtVodDataRow row) {
        return !Strings.isNullOrEmpty(getBrandId(row));
    }

    private boolean canParseBrandFromEpisode(BtVodDataRow row) {
        String seriesNumber = row.getColumnValue(BtVodFileColumn.SERIES_NUMBER);
        String isSeries = row.getColumnValue(BtVodFileColumn.IS_SERIES);
        return seriesNumber != null
                && Ints.tryParse(seriesNumber) != null
                && !"Y".equals(isSeries)
                && isTitleSyntesizableFromEpisode(row.getColumnValue(BtVodFileColumn.PRODUCT_TITLE));
    }

    private boolean canParseBrandFromSeries(BtVodDataRow row) {
        String isSeries = row.getColumnValue(BtVodFileColumn.IS_SERIES);
        return "Y".equals(isSeries)
                && isTitleSynthesizableFromSeries(row.getColumnValue(BtVodFileColumn.PRODUCT_TITLE));
    }

    private boolean isTitleSynthesizableFromSeries(String title) {
        Matcher matcher = BRAND_TITLE_FROM_SERIES_PATTERN.matcher(title);
        return matcher.matches();
    }

    private boolean isTitleSyntesizableFromEpisode(String title) {
        Matcher matcher = BRAND_TITLE_FROM_EPISODE_PATTERN.matcher(title);
        return matcher.matches();
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

    private Brand brandFrom(BtVodDataRow row) {
        Brand brand = new Brand(uriFor(row).get(), null, publisher);
        String productTitle = row.getColumnValue(BtVodFileColumn.PRODUCT_TITLE);

        if (isBrand(row)) {
            brand.setTitle(row.getColumnValue(BtVodFileColumn.BRAND_TITLE));
        } else if (canParseBrandFromEpisode(row)) {
            brand.setTitle(brandTitleFromEpisodeTitle(productTitle));
        } else if (canParseBrandFromSeries(row)) {
            brand.setTitle(brandTitleFromSeriesTitle(productTitle));
        } else {
            String productId = row.getColumnValue(BtVodFileColumn.PRODUCT_ID);
            throw new RuntimeException("Unexpected state - row with product_id: " + productId + " is not a brand nor is possible to parse a brand from it");
        }

        describedFieldsExtractor.setDescribedFieldsFrom(row, brand);
        return brand;
    }
    
    private Optional<String> uriFor(BtVodDataRow row) {
        if (!shouldSynthesizeBrand(row) && !isBrand(row)) {
            return Optional.absent();
        } else if (shouldSynthesizeBrand(row)) {
            return Optional.of(uriPrefix + "synthesized/brands/" + ensureSynthesizedKey(row));
        } else if (isBrand(row)) {
            return Optional.of(uriPrefix + "brands/" + getBrandId(row));
        }

        return Optional.absent();
    }

    private String getBrandId(BtVodDataRow row) {
        return row.getColumnValue(BtVodFileColumn.BRANDIA_ID);
    }

    private String ensureSynthesizedKey(BtVodDataRow row) {
        Optional<String> synthesizedKey = getSynthesizedKey(row);

        if (!synthesizedKey.isPresent()) {
            String productId = row.getColumnValue(BtVodFileColumn.PRODUCT_ID);
            throw new RuntimeException("Not able to generate a synthesized key for a synthesizable brand from row: [PRODUCT_ID="+productId+"]");
        }

        return synthesizedKey.get();
    }

    @Override
    public UpdateProgress getResult() {
        return progress;
    }

    public Optional<ParentRef> getBrandRefFor(BtVodDataRow row) {
        Optional<String> optionalUri = uriFor(row);

        if (!optionalUri.isPresent()) {
            return Optional.absent();
        }

        return Optional.fromNullable(processedBrands.get(optionalUri.get()));
    }
    
}