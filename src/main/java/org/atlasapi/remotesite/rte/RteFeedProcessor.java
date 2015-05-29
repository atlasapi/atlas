package org.atlasapi.remotesite.rte;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.List;

import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Elements;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.remotesite.ContentMerger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.StatusReporter;


public class RteFeedProcessor {
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final ContentWriter contentWriter;
    private final ContentResolver contentResolver;
    private final ContentMerger contentMerger;
    private final RteBrandExtractor brandExtractor;
    private final ContentLister contentLister;
    

    public RteFeedProcessor(ContentWriter contentWriter, ContentResolver contentResolver, 
            ContentMerger contentMerger, ContentLister contentLister, 
            RteBrandExtractor brandExtractor) {
        this.contentWriter = checkNotNull(contentWriter);
        this.contentResolver = checkNotNull(contentResolver);
        this.contentMerger = checkNotNull(contentMerger);
        this.brandExtractor = checkNotNull(brandExtractor);
        this.contentLister = checkNotNull(contentLister);
    }

    public void process(Document document, StatusReporter statusReporter) {
        int processed = 0;
        int failed = 0;
        
        Elements docs = document.getRootElement()
                                .getFirstChildElement("result")
                                .getChildElements();
        
        List<String> seenUris = Lists.newArrayList();
        for (int i = 0; i < docs.size(); i++) {
            Element element = docs.get(i);
            try {
                Brand extractedBrand = brandExtractor.extract(element);
                seenUris.add(extractedBrand.getCanonicalUri());
                contentWriter.createOrUpdate(resolveAndMerge(extractedBrand));
                processed++;
            } catch (Exception e) {
                log.error("Error while processing feed entry: " + element, e);
                failed++;
            }
            statusReporter.reportStatus(statusMessage(processed, failed));
        }
        removeRelatedLinksOnContentNotSeen(seenUris);
    }

    private void removeRelatedLinksOnContentNotSeen(List<String> seenUris) {
        Iterator<Content> allContent = 
                    contentLister.listContent(new ContentListingCriteria
                                                    .Builder()
                                                    .forPublisher(Publisher.RTE)
                                                    .build()
                                             );
        
        while (allContent.hasNext()) {
            Brand brand = (Brand) allContent.next();
            log.trace("Processing content for stale related link removal {}", brand.getCanonicalUri());
            if (!seenUris.contains(brand.getCanonicalUri())) {
                log.trace("Removing related for {}, since we didn't see this content in the feed", brand.getCanonicalUri());
                brand.setRelatedLinks(ImmutableSet.<RelatedLink>of());
                contentWriter.createOrUpdate(brand);
            } else {
                log.trace("Not removing related links for {}, since we saw this content in the feed", brand.getCanonicalUri());
            }
        }
    }

    private Container resolveAndMerge(Brand extractedBrand) {
        Maybe<Identified> resolvedBrand = contentResolver.findByCanonicalUris(canonicalUrisFor(extractedBrand))
                .getFirstValue();

        if (!resolvedBrand.hasValue()) {
            return extractedBrand;
        }

        return contentMerger.merge((Brand) resolvedBrand.requireValue(), extractedBrand);
    }

    private ImmutableList<String> canonicalUrisFor(Brand extractedBrand) {
        return ImmutableList.of(extractedBrand.getCanonicalUri());
    }
    
    private String statusMessage(int processed, int failed) {
        return String.format("Number of entries processed: %d - Number of entries failed: %d",
                processed,
                failed);
    }

}
