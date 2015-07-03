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

    private static final String HELP_TYPE = "help";
    private static final String EPISODE_TYPE = "episode";
    
    static final List<Pattern> BRAND_TITLE_FROM_EPISODE_PATTERNS = ImmutableList.of(
            Pattern.compile("^(.*):.*S[0-9]+.*S[0-9]+\\-E.*"),
            Pattern.compile("^(.*).*S[0-9]+\\-E.*"),
            Pattern.compile("^(.*)Season\\s[0-9]+\\s-\\sSeason\\s[0-9]+\\sEpisode\\s[0-9]+.*"),
            Pattern.compile("^(.*)\\-.*")
    );
    
    static final Pattern BRAND_TITLE_FROM_SERIES_PATTERN = Pattern.compile("^(.*) Series [0-9]+");

    static final Pattern HD_PATTERN = Pattern.compile("^(.*)\\-\\sHD");

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
    private final TitleSanitiser titleSanitiser;
    private final BtVodDescribedFieldsExtractor describedFieldExtractor;
    private final BrandImageExtractor brandImageExtractor;
    private final BrandUriExtractor brandUriExtractor;
    private UpdateProgress progress = UpdateProgress.START;

    public BtVodBrandWriter(
            ContentWriter writer,
            ContentResolver resolver,
            Publisher publisher,
            String uriPrefix,
            BtVodContentListener listener,
            Set<String> processedRows,
            TitleSanitiser titleSanitiser,
            BtVodDescribedFieldsExtractor describedFieldExtractor,
            BrandImageExtractor brandImageExtractor,
            BrandUriExtractor brandUriExtractor) {
        this.brandImageExtractor = checkNotNull(brandImageExtractor);
        this.listener = checkNotNull(listener);
        this.writer = checkNotNull(writer);
        this.resolver = checkNotNull(resolver);
        this.publisher = checkNotNull(publisher);
        this.uriPrefix = checkNotNull(uriPrefix);
        this.contentMerger = new ContentMerger(MergeStrategy.REPLACE, MergeStrategy.REPLACE, MergeStrategy.REPLACE);
        this.processedRows = checkNotNull(processedRows);
        this.titleSanitiser = checkNotNull(titleSanitiser);
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

    private boolean shouldSynthesizeBrand(BtVodEntry row) {
        return !HELP_TYPE.equals(row.getProductType())
                && EPISODE_TYPE.equals(row.getProductType())
                && canParseBrandFromEpisode(row);
    }
    
    private boolean canParseBrandFromEpisode(BtVodEntry row) {
        return isTitleSyntesizableFromEpisode(row.getTitle());
    }
    
    //TODO remove this duplication of code
    private boolean isTitleSyntesizableFromEpisode(String title) {
        for (Pattern brandPattern : BtVodBrandWriter.BRAND_TITLE_FROM_EPISODE_PATTERNS) {
            if (brandPattern.matcher(stripHDSuffix(title)).matches()) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isBrandAlreadyProcessed(BtVodEntry row) {
        Optional<String> optionalUri = brandUriFor(row);
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
            Matcher matcher = brandPattern.matcher(stripHDSuffix(title));
            if (matcher.matches()) {
                return titleSanitiser.sanitiseTitle(matcher.group(1));
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

        if (canParseBrandFromEpisode(row)) {
            brand.setTitle(brandTitleFromEpisodeTitle(row.getTitle()));
        } else {
            String productId = row.getGuid();
            throw new RuntimeException("Unexpected state - row with product_id: " + productId + " is not a brand nor is possible to parse a brand from it");
        }
        brand.setAliases(describedFieldExtractor.aliasesFrom(row));
        brand.setGenres(describedFieldExtractor.btGenreStringsFrom(row));
        brand.addTopicRefs(describedFieldExtractor.topicFor(row).asSet());
        brand.setSpecialization(Specialization.TV);
        brand.setImages(brandImageExtractor.extractImages(row));
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