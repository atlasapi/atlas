package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.metabroadcast.common.scheduling.UpdateProgress;

/**
 * Create a {@link Brand} from a {@link BtVodEntry}. This may either be
 * from brand information contained explicitly in a row, or synthesized
 * from episode data
 *
 * @author tom
 *
 */
public class BtVodBrandExtractor implements BtVodDataProcessor<UpdateProgress> {

    static final Pattern HD_PATTERN = Pattern.compile("^(.*)\\-\\sHD");

    private static final Logger log = LoggerFactory.getLogger(BtVodBrandExtractor.class);
    private static final boolean CONTINUE = true;

    private final Map<String, Brand> processedBrands = Maps.newHashMap();

    private final Publisher publisher;
    private final BtVodContentListener listener;
    private final Set<String> processedRows;
    private final BtVodDescribedFieldsExtractor describedFieldExtractor;
    private final BrandImageExtractor brandImageExtractor;
    private final BrandUriExtractor brandUriExtractor;
    private UpdateProgress progress = UpdateProgress.START;
    public BtVodBrandExtractor(
            Publisher publisher,
            BtVodContentListener listener,
            Set<String> processedRows,
            BtVodDescribedFieldsExtractor describedFieldExtractor,
            BrandImageExtractor brandImageExtractor,
            BrandUriExtractor brandUriExtractor
    ) {
        this.brandImageExtractor = checkNotNull(brandImageExtractor);
        this.listener = checkNotNull(listener);
        this.publisher = checkNotNull(publisher);
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
            listener.onContent(brand, row);
            processedBrands.put(brand.getCanonicalUri(), brand);
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
        Optional<String> optionalUri = brandUriExtractor.extractBrandUri(row);
        return optionalUri.isPresent() && processedBrands.containsKey(optionalUri.get());
    }

    private String getKey(BtVodEntry row) {
        String productId = row.getGuid();
        return brandUriExtractor.getSynthesizedKey(row).or(productId);
    }

    private Brand brandFrom(BtVodEntry row) {
        Brand brand = new Brand(brandUriExtractor.extractBrandUri(row).get(), null, publisher);
        if (brandUriExtractor.canParseBrandFromEpisode(row)) {
            brand.setTitle(brandUriExtractor.brandTitleFromEpisodeTitle(row.getTitle()));
        } else if (brandUriExtractor.canParseBrandFromSeries(row)) {
            brand.setTitle(brandUriExtractor.brandTitleFromSeriesTitle(row.getTitle()));
        } else {
            String productId = row.getGuid();
            throw new RuntimeException("Unexpected state - row with product_id: " + productId + " is not a brand nor is possible to parse a brand from it");
        }
        brand.setAliases(describedFieldExtractor.aliasesFrom(row));
        brand.setGenres(describedFieldExtractor.btGenreStringsFrom(row));
        brand.setSpecialization(Specialization.TV);
        brand.setImages(brandImageExtractor.imagesFor(row));

        VodEntryAndContent vodEntryAndContent = new VodEntryAndContent(row, brand);
        brand.addTopicRefs(describedFieldExtractor.topicsFrom(vodEntryAndContent));
        return brand;
    }



    @Override
    public UpdateProgress getResult() {
        return progress;
    }

    public Map<String, Brand> getProcessedBrands() {
        return ImmutableMap.copyOf(processedBrands);
    }

}