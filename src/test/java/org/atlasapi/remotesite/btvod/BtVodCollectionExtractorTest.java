package org.atlasapi.remotesite.btvod;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.atlasapi.media.entity.Image;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;

@RunWith(MockitoJUnitRunner.class)
public class BtVodCollectionExtractorTest {

    private @Mock BtVodBrandProvider brandProvider;
    private @Mock ImageExtractor imageExtractor;

    private BtVodCollectionExtractor collectionExtractor;

    @Before
    public void setUp() throws Exception {
        collectionExtractor = new BtVodCollectionExtractor(brandProvider, imageExtractor);
    }

    @Test
    public void testProcessCollection() throws Exception {
        DateTime dateTime = DateTime.now(DateTimeZone.UTC);

        BtVodEntry row = getRow(dateTime);

        Image image = new Image("image");
        when(imageExtractor.imagesFor(row)).thenReturn(ImmutableSet.of(image));

        collectionExtractor.process(row);

        assertThat(collectionExtractor.getResult().getProcessed(), is(1));

        verify(brandProvider).updateBrandFromCollection(
                new BtVodCollection(
                        row.getGuid(),
                        dateTime,
                        row.getDescription(),
                        row.getProductLongDescription(),
                        ImmutableSet.of(image)
                )
        );
    }

    @Test
    public void testDoNotProcessIfNotOfTypeCollection() throws Exception {
        BtVodEntry row = new BtVodEntry();
        row.setProductType("season");

        collectionExtractor.process(row);

        assertThat(collectionExtractor.getResult().getProcessed(), is(1));

        verify(brandProvider, never()).updateBrandFromCollection(any(BtVodCollection.class));
    }

    private BtVodEntry getRow(DateTime dateTime) {
        BtVodEntry row = new BtVodEntry();

        row.setGuid("guid");
        row.setProductType("collection");
        row.setAdded(dateTime.getMillis());
        row.setDescription("description");
        row.setProductLongDescription("longDescription");

        return row;
    }
}