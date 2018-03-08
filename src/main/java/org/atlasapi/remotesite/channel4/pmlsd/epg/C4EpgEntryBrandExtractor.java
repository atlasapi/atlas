package org.atlasapi.remotesite.channel4.pmlsd.epg;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.channel4.pmlsd.C4AtomApi;
import org.atlasapi.remotesite.channel4.pmlsd.ContentFactory;
import org.atlasapi.remotesite.channel4.pmlsd.epg.model.C4EpgEntry;

import java.util.Optional;

public class C4EpgEntryBrandExtractor implements ContentExtractor<C4EpgEntry, Optional<Brand>> {

    private final ContentFactory<C4EpgEntry, C4EpgEntry, C4EpgEntry> contentFactory;
    
    public C4EpgEntryBrandExtractor(ContentFactory<C4EpgEntry, C4EpgEntry, C4EpgEntry> contentFactory) {
        this.contentFactory = contentFactory;
    }
    
    @Override
    public Optional<Brand> extract(C4EpgEntry source) {
        if (!source.hasRelatedLink()) {
            return Optional.empty();
        }
        
        Optional<Brand> possibleBrand = contentFactory.createBrand(source);
        if (!possibleBrand.isPresent()) {
            return Optional.empty();
        }
        
        Brand brand = possibleBrand.get();
        
        brand.addAliasUrl(C4AtomApi.hierarchyUriFromCanonical(brand.getCanonicalUri()));
        
        if (source.brandTitle() != null) {
            brand.setTitle(source.brandTitle());
        } else {
            brand.setTitle(source.title());
        }
        
        return Optional.of(brand);
    }

}
