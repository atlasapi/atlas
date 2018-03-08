package org.atlasapi.remotesite.channel4.pmlsd.epg;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.remotesite.channel4.pmlsd.C4AtomContentResolver;
import org.atlasapi.remotesite.channel4.pmlsd.C4UriExtractor.C4UriAndAliases;
import org.atlasapi.remotesite.channel4.pmlsd.epg.model.C4EpgEntry;

import java.util.Optional;

public class C4EpgEntryContentResolver {

    private final C4AtomContentResolver resolver;
    private final C4EpgEntryUriExtractor uriExtractor = new C4EpgEntryUriExtractor();
    private final Publisher publisher;
    
    public C4EpgEntryContentResolver(ContentResolver resolver, Publisher publisher) {
        this.resolver = new C4AtomContentResolver(resolver);
        this.publisher = publisher;
    }
    
    public Optional<Brand> resolveBrand(C4EpgEntry entry) {
        return uriExtractor.uriForBrand(publisher, entry)
                .map(C4UriAndAliases::getUri)
                .flatMap(resolver::brandFor);
    }
    
    public Optional<Series> resolveSeries(C4EpgEntry entry) {
        return uriExtractor.uriForSeries(publisher, entry)
                .map(C4UriAndAliases::getUri)
                .flatMap(resolver::seriesFor);
    }

    public Optional<Item> itemFor(C4EpgEntry entry) {
        return uriExtractor.uriForItem(publisher, entry)
                .map(C4UriAndAliases::getUri)
                .flatMap(resolver::itemFor);
    }

}
