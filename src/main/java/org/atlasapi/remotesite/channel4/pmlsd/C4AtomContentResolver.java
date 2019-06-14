package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.List;
import java.util.Optional;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class C4AtomContentResolver {

    private final ContentResolver resolver;

    public C4AtomContentResolver(ContentResolver resolver) {
        this.resolver = resolver;
    }
    
    public Optional<Item> itemFor(String id) {
        List<Identified> results = resolver.findByCanonicalUris(Lists.newArrayList(id)).getAllResolvedResults();
        Iterable<Item> itemFilteredResults = ImmutableSet.copyOf(Iterables.filter(results, Item.class));
        return Optional.ofNullable(Iterables.getOnlyElement(itemFilteredResults,null));
    }
    
    public Optional<Brand> brandFor(String canonicalUri) {
        return resolver.findByCanonicalUris(ImmutableList.of(canonicalUri)).get(canonicalUri)
                .toOptional()
                .filter(Brand.class::isInstance)
                .map(Brand.class::cast);
    }
    
    public Optional<Series> seriesFor(String canonicalUri) {
        return resolver.findByCanonicalUris(ImmutableList.of(canonicalUri)).get(canonicalUri)
                .toOptional()
                .filter(Series.class::isInstance)
                .map(Series.class::cast);
    }
}
