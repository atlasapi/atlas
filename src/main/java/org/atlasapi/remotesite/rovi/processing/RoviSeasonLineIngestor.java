package org.atlasapi.remotesite.rovi.processing;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.remotesite.rovi.RoviCanonicalUriGenerator.canonicalUriForSeasonHistory;

import java.nio.charset.Charset;

import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.remotesite.rovi.RoviContentWriter;
import org.atlasapi.remotesite.rovi.indexing.IndexAccessException;
import org.atlasapi.remotesite.rovi.model.RoviSeasonHistoryLine;
import org.atlasapi.remotesite.rovi.parsers.RoviSeasonHistoryLineParser;
import org.atlasapi.remotesite.rovi.populators.SeriesPopulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.base.Maybe;


public class RoviSeasonLineIngestor extends RoviActionLineIngestor<RoviSeasonHistoryLine, Series> {

    private final static Logger LOG = LoggerFactory.getLogger(RoviSeasonLineIngestor.class); 
    
    private final ContentResolver contentResolver;
    private final LoadingCache<String, Optional<Publisher>> parentPublisherCache;
    
    public RoviSeasonLineIngestor(RoviSeasonHistoryLineParser parser, Charset charset,
            ContentResolver contentResolver, RoviContentWriter contentWriter, LoadingCache<String, Optional<Publisher>> parentPublisherCache) {
        super(parser, charset, contentWriter);
        this.contentResolver = checkNotNull(contentResolver);
        this.parentPublisherCache = checkNotNull(parentPublisherCache);
    }
    
    @Override
    protected Logger log() {
        return LOG;
    }

    @Override
    protected boolean shouldProcess(RoviSeasonHistoryLine parsedLine) {
        return true;
    }

    private SeriesPopulator populator(RoviSeasonHistoryLine parsedLine) {
        return new SeriesPopulator(parsedLine, parentPublisherCache);
    }

    @Override
    protected Series populateContent(Series content, RoviSeasonHistoryLine parsedLine)
            throws IndexAccessException {
        populator(parsedLine).populateContent(content);
        return content;
    }

    @Override
    protected Optional<Series> resolveContent(RoviSeasonHistoryLine parsedLine) {
        // Using alias for resolving here because for deletion records the seasonId is not present
        String seasonHistoryUri = canonicalUriForSeasonHistory(parsedLine.getSeasonHistoryId());
        
        Maybe<Identified> maybeResolved = contentResolver
                .findByUris(ImmutableList.of(seasonHistoryUri))
                .getFirstValue();
        
        if (maybeResolved.isNothing()) {
            return Optional.absent();
        }
        
        return Optional.of((Series) maybeResolved.requireValue());
    }

    @Override
    protected Series createContent(RoviSeasonHistoryLine parsedLine) {
        return new Series();
    }

}
