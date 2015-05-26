package org.atlasapi.remotesite.btvod;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import org.mockito.Mockito;
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

    private final BtVodBrandWriter brandExtractor
                    = new BtVodBrandWriter(
                                contentWriter,
                                contentResolver,
                                PUBLISHER, URI_PREFIX,
                                contentListener,
                                Sets.<String>newHashSet(),
            new TitleSanitiser()
    );
    
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

    private BtVodEntry episodeEntry() {
        BtVodEntry entry = new BtVodEntry();
        entry.setProductType("episode");
        return entry;
    }

    @Test
    public void testCanParseBrandFromEpisodeTitles() {
        BtVodEntry row1 = episodeEntry();
        row1.setTitle("Cashmere Mafia S1-E2 Conference Call");

        BtVodEntry row2 = episodeEntry();
        row2.setTitle(FULL_EPISODE_TITLE);

        BtVodEntry row3 = episodeEntry();
        row3.setTitle("Classic Premiership Rugby - Saracens v Leicester Tigers 2010/11");

        BtVodEntry row4 = episodeEntry();
        row4.setTitle("UFC: The Ultimate Fighter Season 19 - Season 19 Episode 2");

        BtVodEntry row5 = episodeEntry();
        row5.setTitle("Modern Family: S03 - HD S3-E17 Truth Be Told - HD");

        BtVodEntry row6 = episodeEntry();
        row6.setTitle("Being Human (USA) S2-E7 The Ties That Blind");

        BtVodEntry row7 = episodeEntry();
        row7.setTitle("ZQWModern_Family: S01 S1-E4 ZQWThe Incident");

        BtVodEntry row8 = episodeEntry();
        row8.setTitle("ZQZPeppa Pig: S01 S1-E4 ZQZSchool Play");

        BtVodEntry row9 = episodeEntry();
        row9.setTitle("ZQWAmerican_Horror_Story: S01 S1-E11 ZQWBirth");

        BtVodEntry row10 = episodeEntry();
        row10.setTitle("The Hunchback of Notre Dame II (Disney) - HD");

        BtVodEntry row11 = episodeEntry();
        row11.setTitle("Classic Premiership Rugby - Saracens v Leicester Tigers 2010/11 - HD");


        assertThat(brandExtractor.uriFor(row1).get(), is(URI_PREFIX + "synthesized/brands/cashmere-mafia"));
        assertThat(brandExtractor.uriFor(row2).get(), is(URI_PREFIX + "synthesized/brands/brand-title"));
        assertThat(brandExtractor.uriFor(row3).get(), is(URI_PREFIX + "synthesized/brands/classic-premiership-rugby"));
        assertThat(brandExtractor.uriFor(row4).get(), is(URI_PREFIX + "synthesized/brands/ufc-the-ultimate-fighter"));
        assertThat(brandExtractor.uriFor(row5).get(), is(URI_PREFIX + "synthesized/brands/modern-family"));
        assertThat(brandExtractor.uriFor(row6).get(), is(URI_PREFIX + "synthesized/brands/being-human-usa"));
        assertThat(brandExtractor.uriFor(row7).get(), is(URI_PREFIX + "synthesized/brands/modern-family"));
        assertThat(brandExtractor.uriFor(row8).get(), is(URI_PREFIX + "synthesized/brands/peppa-pig"));
        assertThat(brandExtractor.uriFor(row9).get(), is(URI_PREFIX + "synthesized/brands/american-horror-story"));
        assertThat(brandExtractor.uriFor(row10).isPresent(), is(false));
        assertThat(brandExtractor.uriFor(row11).get(), is(URI_PREFIX + "synthesized/brands/classic-premiership-rugby"));
    }

    @Test
    public void testDoesntCreateSyntheticBrandFromNonEpisodeData() {
        when(imageUriProvider.imageUriFor(Matchers.anyString())).thenReturn(Optional.<String>absent());
        when(contentResolver.findByCanonicalUris(ImmutableSet.of(URI_PREFIX + "synthesized/brands/brand-title")))
                .thenReturn(ResolvedContent.builder().build());

        BtVodEntry entry = row();
        entry.setProductType("film");

        brandExtractor.process(entry);

        verify(contentWriter,never()).createOrUpdate(Mockito.any(Brand.class));
    }

    private BtVodEntry row() {
        BtVodEntry entry = new BtVodEntry();
        entry.setGuid(PRODUCT_ID);
        entry.setTitle(FULL_EPISODE_TITLE);
        entry.setProductOfferStartDate(1364774400000L); //"Apr  1 2013 12:00AM"
        entry.setProductOfferEndDate(1398816000000L);// "Apr 30 2014 12:00AM"
        entry.setProductType("episode");
        return entry;
    }
    

    private String brandUri() {
        return URI_PREFIX + "brands/" + PRODUCT_ID;
    }
}
