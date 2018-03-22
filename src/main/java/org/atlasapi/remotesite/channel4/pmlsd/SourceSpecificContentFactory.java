package org.atlasapi.remotesite.channel4.pmlsd;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;

import com.google.common.base.Objects;
import org.atlasapi.remotesite.channel4.pmlsd.C4UriExtractor.C4UriAndAliases;

import java.util.Optional;

public class SourceSpecificContentFactory<B, S, I> implements ContentFactory<B, S, I> {

    private final Publisher source;
    private final C4UriExtractor<B, S, I> uriExtractor;

    public SourceSpecificContentFactory(Publisher source, C4UriExtractor<B, S, I> uriExtractor) {
        this.source = checkNotNull(source);
        this.uriExtractor = checkNotNull(uriExtractor);
    }
    
    @Override
    public Optional<Brand> createBrand(B remote) {
        return defaulted(new Brand(), uriExtractor.uriForBrand(source, remote));        
    }
    
    @Override
    public Optional<Episode> createEpisode(I remote) {
        return defaulted(new Episode(), uriExtractor.uriForItem(source, remote));
    }
    
    @Override
    public Optional<Item> createItem(I remote) {
        return defaulted(new Item(), uriExtractor.uriForItem(source, remote));
    }
    
    @Override
    public Optional<Series> createSeries(S remote) {
        return defaulted(new Series(), uriExtractor.uriForSeries(source, remote));
    }
    
    @Override
    public Optional<Clip> createClip(I remote) {
        return defaulted(new Clip(), uriExtractor.uriForClip(source, remote));
    }
    
    /**
     * If a URI is present, then a defaulted {@link Described} can be created. Otherwise,
     * it's not possible to create one.
     * 
     * @param described
     * @param uri
     * @return
     */
    private <A extends Described> Optional<A> defaulted(
            A described,
            Optional<C4UriAndAliases> uri
    ) {
        if (!uri.isPresent()) {
            return Optional.empty();
        }
        
        described.setCanonicalUri(uri.get().getUri());
        described.setPublisher(source);
        described.addAliasUrls(uri.get().getAliasUrls());
        described.addAliases(uri.get().getAliases());
        return Optional.of(described);
    }
    
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof SourceSpecificContentFactory) {
            SourceSpecificContentFactory<?, ?, ?> other = (SourceSpecificContentFactory<?, ?, ?>) that;
            return Objects.equal(source, other.source) 
                    && Objects.equal(uriExtractor, other.uriExtractor);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(source, uriExtractor);
    }
    
    @Override
    public String toString() {
        return String.format("%s (%s) content factory", source.title(), source.key());
    }

}
