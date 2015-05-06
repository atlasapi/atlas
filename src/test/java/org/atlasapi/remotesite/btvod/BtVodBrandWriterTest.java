package org.atlasapi.remotesite.btvod;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

@RunWith(MockitoJUnitRunner.class)
public class BtVodBrandWriterTest {

    private static final String BRAND_TITLE = "Brand Title";
    private static final String PRODUCT_ID = "1234";
    private static final String REAL_EPISODE_TITLE = "Real Title";
    private static final String FULL_EPISODE_TITLE = BRAND_TITLE + ": S1 S1-E9 " + REAL_EPISODE_TITLE;
    private static final Publisher PUBLISHER = Publisher.BT_VOD;
    private static final String URI_PREFIX = "http://example.org/";
    
    private final ContentWriter contentWriter = mock(ContentWriter.class);
    private final ContentResolver contentResolver = mock(ContentResolver.class);
    private final BtVodContentListener contentListener = mock(BtVodContentListener.class);
    private final ImageUriProvider imageUriProvider = mock(ImageUriProvider.class);
    private final BtVodDescribedFieldsExtractor extractor = mock(BtVodDescribedFieldsExtractor.class);
    
    private final BtVodBrandWriter brandExtractor 
                    = new BtVodBrandWriter(
                                contentWriter, 
                                contentResolver, 
                                PUBLISHER, URI_PREFIX,
                                contentListener,
                                extractor,
                                Sets.<String>newHashSet());
    
    @Test
    public void testCreatesSyntheticBrandFromEpisodeData() {
        when(imageUriProvider.imageUriFor(Matchers.anyString())).thenReturn(Optional.<String>absent());
        when(contentResolver.findByCanonicalUris(ImmutableSet.of(URI_PREFIX + "synthesized/brands/brand-title")))
                .thenReturn(ResolvedContent.builder().build());

        brandExtractor.process(row());

        ArgumentCaptor<Brand> captor = ArgumentCaptor.forClass(Brand.class);
        verify(contentWriter).createOrUpdate(captor.capture());

        Brand saved = captor.getValue();
        assertThat(saved.getCanonicalUri(), is(URI_PREFIX + "synthesized/brands/brand-title"));
        assertThat(saved.getTitle(), is(BRAND_TITLE));
    }

    private BtVodEntry row() {
        BtVodEntry entry = new BtVodEntry();
        entry.setGuid(PRODUCT_ID);
        entry.setTitle(FULL_EPISODE_TITLE);
        entry.setPlproduct$offerStartDate(1364774400000L); //"Apr  1 2013 12:00AM"
        entry.setPlproduct$offerEndDate(1398816000000L);// "Apr 30 2014 12:00AM"
        return entry;
    }
    

    private String brandUri() {
        return URI_PREFIX + "brands/" + PRODUCT_ID;
    }
}
