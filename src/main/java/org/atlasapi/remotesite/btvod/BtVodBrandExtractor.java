package org.atlasapi.remotesite.btvod;

import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.metabroadcast.common.scheduling.UpdateProgress;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Create a {@link Brand} from a {@link BtVodEntry}. This may either be
 * from brand information contained explicitly in a row, or synthesized
 * from episode data
 *
 * @author tom
 *
 */
public class BtVodBrandExtractor implements BtVodDataProcessor<UpdateProgress> {

    private static final Logger log = LoggerFactory.getLogger(BtVodBrandExtractor.class);
    private static final boolean CONTINUE = true;

    private final Map<String, Brand> processedBrands = Maps.newHashMap();
    private final Map<String, Brand> parentGuidToBrand = Maps.newHashMap();

    private final Publisher publisher;
    private final BtVodContentListener listener;
    private final Set<String> processedRows;
    private final BtVodDescribedFieldsExtractor describedFieldExtractor;
    private final BrandUriExtractor brandUriExtractor;
    private UpdateProgress progress = UpdateProgress.START;
    private final BtVodTagMap btVodTagMap;

    public BtVodBrandExtractor(
            Publisher publisher,
            BtVodContentListener listener,
            Set<String> processedRows,
            BtVodDescribedFieldsExtractor describedFieldExtractor,
            BrandUriExtractor brandUriExtractor,
            BtVodTagMap btVodTagMap
    ) {
        this.listener = checkNotNull(listener);
        this.publisher = checkNotNull(publisher);
        this.processedRows = checkNotNull(processedRows);
        this.brandUriExtractor = checkNotNull(brandUriExtractor);
        this.describedFieldExtractor = checkNotNull(describedFieldExtractor);
        this.btVodTagMap = btVodTagMap;
    }

    @Override
    public boolean process(BtVodEntry row) {
        UpdateProgress thisProgress = UpdateProgress.FAILURE;
        try {
            if (!brandUriExtractor.shouldSynthesizeBrand(row)
                    || processedRows.contains(getKey(row))) {
                thisProgress = UpdateProgress.SUCCESS;
                return CONTINUE;
            }

            Brand brand;
            Optional<String> optionalUri = brandUriExtractor.extractBrandUri(row);

            if (optionalUri.isPresent() && processedBrands.containsKey(optionalUri.get())) {
                brand = processedBrands.get(optionalUri.get());
            }
            else {
                brand = brandFrom(row, optionalUri.get());
            }

            listener.onContent(brand, row);
            processedBrands.put(brand.getCanonicalUri(), brand);
            updateParentGuidToBrand(row, brand);

            thisProgress = UpdateProgress.SUCCESS;
        } catch (Exception e) {
            log.error("Failed to process row " + row.toString(), e);
        }
        finally {
            progress = progress.reduce(thisProgress);
        }
        return CONTINUE;
    }

    private void updateParentGuidToBrand(BtVodEntry row, Brand brand) {
        if (row.getParentGuid() != null) {
            parentGuidToBrand.put(row.getParentGuid(), brand);
        }
    }

    private String getKey(BtVodEntry row) {
        String productId = row.getGuid();
        return brandUriExtractor.getSynthesizedKey(row).or(productId);
    }

    private Brand brandFrom(BtVodEntry row, String brandUri) {
        Brand brand = new Brand(brandUri, null, publisher);
        if (brandUriExtractor.canParseBrandFromEpisode(row)) {
            brand.setTitle(brandUriExtractor.brandTitleFromEpisodeTitle(row.getTitle()));
        } else if (brandUriExtractor.canParseBrandFromSeries(row)) {
            brand.setTitle(brandUriExtractor.brandTitleFromSeriesTitle(row.getTitle()));
        } else {
            String productId = row.getGuid();
            throw new RuntimeException("Unexpected state - row with product_id: " + productId +
                    " is not a brand nor is possible to parse a brand from it");
        }
        brand.setAliases(describedFieldExtractor.synthesisedAliasesFrom(row));
        brand.setGenres(describedFieldExtractor.btGenreStringsFrom(row));
        brand.setSpecialization(Specialization.TV);

        VodEntryAndContent vodEntryAndContent = new VodEntryAndContent(row, brand);
        brand.addTopicRefs(describedFieldExtractor.topicsFrom(vodEntryAndContent));
        brand.addTopicRefs(btVodTagMap.mapGenresToTopicRefs(brand.getGenres()));
        return brand;
    }

    @Override
    public UpdateProgress getResult() {
        return progress;
    }

    public Map<String, Brand> getProcessedBrands() {
        return ImmutableMap.copyOf(processedBrands);
    }

    public Map<String, Brand> getParentGuidToBrand() {
        return ImmutableMap.copyOf(parentGuidToBrand);
    }
}
