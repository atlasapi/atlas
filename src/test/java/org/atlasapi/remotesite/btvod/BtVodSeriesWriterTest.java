package org.atlasapi.remotesite.btvod;


import com.google.api.client.util.Sets;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.junit.Test;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class BtVodSeriesWriterTest {

    private static final Publisher PUBLISHER = Publisher.BT_VOD;

    private final ContentWriter contentWriter = mock(ContentWriter.class);
    private final ContentResolver contentResolver = mock(ContentResolver.class);
    private final BtVodBrandWriter brandExtractor = mock(BtVodBrandWriter.class);
    private final BtVodDescribedFieldsExtractor extractor = mock(BtVodDescribedFieldsExtractor.class);
    private final BtVodContentListener contentListener = mock(BtVodContentListener.class);
    private final ImageUriProvider imageUriProvider = mock(ImageUriProvider.class);


    private final BtVodSeriesWriter seriesExtractor = new BtVodSeriesWriter(
            contentWriter,
            contentResolver,
            brandExtractor,
            extractor,
            PUBLISHER,
            contentListener,
            Sets.<String>newHashSet()
    );


    @Test
    public void testExtractsSeriesFromEpisode() {
        fail();
    }

    @Test
    public void testExtractsSeriesFromSeriesCollection() {
        fail();
    }

}