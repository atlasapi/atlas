package org.atlasapi.remotesite.itunes.epf;

import static org.atlasapi.remotesite.itunes.epf.model.EpfCollection.COLLECTION_ID;
import static org.atlasapi.remotesite.itunes.epf.model.EpfCollection.NAME;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.itunes.epf.model.EpfCollection;


public class ItunesCollectionSeriesExtractor implements ContentExtractor<EpfCollection, Series> {

    @Override
    public Series extract(EpfCollection collection) {
        Integer collectionId = collection.get(COLLECTION_ID);
        
        Series series = new Series("http://itunes.apple.com/tv-season/id"+collectionId, "itunes:t-"+collectionId, Publisher.ITUNES);
        series.setTitle(collection.get(NAME));
        
        return series;
    }

}
