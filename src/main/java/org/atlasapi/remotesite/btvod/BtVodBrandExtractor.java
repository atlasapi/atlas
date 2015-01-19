package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import joptsimple.internal.Strings;

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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.UpdateProgress;

public class BtVodBrandExtractor implements BtVodDataProcessor<UpdateProgress> {

    private static final Logger log = LoggerFactory.getLogger(BtVodBrandExtractor.class);
    private static final boolean CONTINUE = true;
    
    private final Map<String, ParentRef> processedBrands = Maps.newHashMap();
    private final ContentWriter writer;
    private final ContentResolver resolver;
    private final Publisher publisher;
    private final String uriPrefix;
    private final ContentMerger contentMerger;
    private final BtVodContentListener listener;
    private UpdateProgress progress = UpdateProgress.START;
    
    public BtVodBrandExtractor(ContentWriter writer, ContentResolver resolver,
            Publisher publisher, String uriPrefix, BtVodContentListener listener) {
        this.listener = checkNotNull(listener);
        this.writer = checkNotNull(writer);
        this.resolver = checkNotNull(resolver);
        this.publisher = checkNotNull(publisher);
        this.uriPrefix = checkNotNull(uriPrefix);
        this.contentMerger = new ContentMerger(MergeStrategy.REPLACE, MergeStrategy.KEEP, MergeStrategy.REPLACE);
    }
    
    @Override
    public boolean process(BtVodDataRow row) {
        UpdateProgress thisProgress = UpdateProgress.FAILURE;
        try {
            String brandId = row.getColumnValue(BtVodFileColumn.BRANDIA_ID);
            if (Strings.isNullOrEmpty(brandId)
                    || processedBrands.containsKey(brandId)) {
                thisProgress = UpdateProgress.SUCCESS;
                return CONTINUE;
            }
            Brand brand = brandFrom(row);
            write(brand);
            listener.onContent(brand, row);
            processedBrands.put(brandId, ParentRef.parentRefFrom(brand));
            thisProgress = UpdateProgress.SUCCESS;
        } catch (Exception e) {
            log.error("Failed to process row " + row.toString(), e);
        }
        finally {
            progress = progress.reduce(thisProgress);
        }
        return CONTINUE;
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
        Brand brand = new Brand(uriFor(row), null, publisher);
        brand.setTitle(row.getColumnValue(BtVodFileColumn.BRAND_TITLE));
        //TOO other fields, e.g. images
        return brand;
    }
    
    private String uriFor(BtVodDataRow row) {
        return uriPrefix + "brands/" + row.getColumnValue(BtVodFileColumn.BRANDIA_ID);
    }

    @Override
    public UpdateProgress getResult() {
        return progress;
    }
    
    public ParentRef getBrandRefFor(String brandIaId) {
        return processedBrands.get(brandIaId);
    }
    
}