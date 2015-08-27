package org.atlasapi.remotesite.btvod;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.topic.TopicCreatingTopicResolver;
import org.atlasapi.persistence.topic.TopicWriter;
import org.atlasapi.remotesite.btvod.contentgroups.BtVodContentMatchingPredicates;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$productTag;
import org.atlasapi.remotesite.btvod.model.BtVodProductScope;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
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
    private static final String BT_VOD_NAMESPECASE_PREFIX = "Namespace prefix";


    private final MergingContentWriter contentWriter = mock(MergingContentWriter.class);
    private final ContentResolver contentResolver = mock(ContentResolver.class);
    private final BtVodContentListener contentListener = mock(BtVodContentListener.class);
    private final ImageUriProvider imageUriProvider = mock(ImageUriProvider.class);
    private final ImageExtractor imageExtractor = mock(ImageExtractor.class);
    private final TopicCreatingTopicResolver topicResolver = mock(TopicCreatingTopicResolver.class);
    private final TopicWriter topicWriter = mock(TopicWriter.class);
    private final BrandUriExtractor brandUriExtractor = new BrandUriExtractor(URI_PREFIX, new TitleSanitiser());
    private final BtVodContentMatchingPredicate newTopicContentMatchingPredicate = mock(BtVodContentMatchingPredicate.class);

    private final BtVodDescribedFieldsExtractor describedFieldsExtractor = new BtVodDescribedFieldsExtractor(
            topicResolver,
            topicWriter,
            Publisher.BT_VOD,
            newTopicContentMatchingPredicate,
            BtVodContentMatchingPredicates.schedulerChannelPredicate("Kids"),
            BtVodContentMatchingPredicates.schedulerChannelAndOfferingTypePredicate(
                    "TV", ImmutableSet.of("Season", "Season-EST")
            ),
            BtVodContentMatchingPredicates.schedulerChannelPredicate("TV Replay"),
            new Topic(123L),
            new Topic(234L),
            new Topic(345L),
            new Topic(456L),
            BT_VOD_NAMESPECASE_PREFIX
    );

    private final BtVodBrandWriter brandExtractor
            = new BtVodBrandWriter(
            PUBLISHER,
            contentListener,
            Sets.<String>newHashSet(),
            describedFieldsExtractor,
            new NoImageExtractor(),
            brandUriExtractor,
            contentWriter

    );
    
    @Test
    public void testCreatesSyntheticBrandFromEpisodeData() {
        BtVodEntry row = row();
        
        when(imageUriProvider.imageUriFor(Matchers.anyString())).thenReturn(Optional.<String>absent());
        when(contentResolver.findByCanonicalUris(ImmutableSet.of(URI_PREFIX + "synthesized/brands/brand-title")))
                .thenReturn(ResolvedContent.builder().build());

        brandExtractor.process(row);

        ArgumentCaptor<Brand> captor = ArgumentCaptor.forClass(Brand.class);
        verify(contentWriter).write(captor.capture());

        Brand saved = captor.getValue();
        assertThat(saved.getCanonicalUri(), is(URI_PREFIX + "synthesized/brands/brand-title"));
        assertThat(saved.getTitle(), is(BRAND_TITLE));
    }

    private BtVodEntry episodeEntry() {
        BtVodEntry entry = new BtVodEntry();
        entry.setProductType("episode");
        return entry;
    }

    private BtVodEntry seasonEntry() {
        BtVodEntry entry = new BtVodEntry();
        entry.setProductType("season");
        return entry;
    }

    @Test
    public void testCanParseBrandFromSeasonTitles() {
        BtVodEntry row1 = seasonEntry();
        row1.setTitle("Dominion: S2");

        BtVodEntry row2 = seasonEntry();
        row2.setTitle("Workaholics: S2 - HD");

        BtVodEntry row3 = seasonEntry();
        row3.setTitle("Wire Series 5");

        BtVodEntry row4 = seasonEntry();
        row4.setTitle("Judge Geordie");

        BtVodEntry row5 = seasonEntry();
        row5.setTitle("Tom and Jerry Series 1");

        BtVodEntry row6 = seasonEntry();
        row6.setTitle("Plankton Invasion - Sr1 Series 1");

        BtVodEntry row7 = seasonEntry();
        row7.setTitle("Transformers Prime: Beast Hunters Series 3");

        assertThat(brandExtractor.brandUriFor(row1).get(), is(URI_PREFIX + "synthesized/brands/dominion"));
        assertThat(brandExtractor.brandUriFor(row2).get(), is(URI_PREFIX + "synthesized/brands/workaholics"));
        assertThat(brandExtractor.brandUriFor(row3).get(), is(URI_PREFIX + "synthesized/brands/wire"));
        assertThat(brandExtractor.brandUriFor(row4).get(), is(URI_PREFIX + "synthesized/brands/judge-geordie"));
        assertThat(brandExtractor.brandUriFor(row5).get(), is(URI_PREFIX + "synthesized/brands/tom-and-jerry"));
        assertThat(brandExtractor.brandUriFor(row6).get(), is(URI_PREFIX + "synthesized/brands/plankton-invasion"));
        assertThat(brandExtractor.brandUriFor(row7).get(), is(URI_PREFIX + "synthesized/brands/transformers-prime-beast-hunters"));
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

        BtVodEntry row12 = episodeEntry();
        row12.setTitle("Plankton Invasion - Operation Yellow-Ball-That-Heats/ Operation Albedo/ Operation Meltdown");

        BtVodEntry row13 = episodeEntry();
        row13.setTitle("Brand - with multiple - dashes");

        BtVodEntry row14 = episodeEntry();
        row14.setTitle("Peppa Pig, Series 2, Vol. 1 - The Quarrel / The Toy Cupboard");

        BtVodEntry row15 = episodeEntry();
        row15.setTitle("Plankton Invasion - Sr1 S1-E46 Operation Some Like It Cold");

        assertThat(brandExtractor.brandUriFor(row1).get(), is(URI_PREFIX + "synthesized/brands/cashmere-mafia"));
        assertThat(brandExtractor.brandUriFor(row2).get(), is(URI_PREFIX + "synthesized/brands/brand-title"));
        assertThat(brandExtractor.brandUriFor(row3).get(), is(URI_PREFIX + "synthesized/brands/classic-premiership-rugby"));
        assertThat(brandExtractor.brandUriFor(row4).get(), is(URI_PREFIX + "synthesized/brands/ufc-the-ultimate-fighter"));
        assertThat(brandExtractor.brandUriFor(row5).get(), is(URI_PREFIX + "synthesized/brands/modern-family"));
        assertThat(brandExtractor.brandUriFor(row6).get(), is(URI_PREFIX + "synthesized/brands/being-human-usa"));
        assertThat(brandExtractor.brandUriFor(row7).get(), is(URI_PREFIX + "synthesized/brands/modern-family"));
        assertThat(brandExtractor.brandUriFor(row8).get(), is(URI_PREFIX + "synthesized/brands/peppa-pig"));
        assertThat(brandExtractor.brandUriFor(row9).get(), is(URI_PREFIX + "synthesized/brands/american-horror-story"));
        assertThat(brandExtractor.brandUriFor(row10).isPresent(), is(false));
        assertThat(brandExtractor.brandUriFor(row11).get(), is(URI_PREFIX + "synthesized/brands/classic-premiership-rugby"));
        assertThat(brandExtractor.brandUriFor(row12).get(), is(URI_PREFIX + "synthesized/brands/plankton-invasion"));
        assertThat(brandExtractor.brandUriFor(row13).get(), is(URI_PREFIX + "synthesized/brands/brand"));
        assertThat(brandExtractor.brandUriFor(row14).get(), is(URI_PREFIX + "synthesized/brands/peppa-pig"));
        assertThat(brandExtractor.brandUriFor(row15).get(), is(URI_PREFIX + "synthesized/brands/plankton-invasion"));
    }

    @Test
    public void testDoesntCreateSyntheticBrandFromNonEpisodeData() {
        when(imageUriProvider.imageUriFor(Matchers.anyString())).thenReturn(Optional.<String>absent());
        when(contentResolver.findByCanonicalUris(ImmutableSet.of(URI_PREFIX + "synthesized/brands/brand-title")))
                .thenReturn(ResolvedContent.builder().build());

        BtVodEntry entry = row();
        entry.setProductType("film");

        brandExtractor.process(entry);

        verify(contentWriter,never()).write(Mockito.any(Brand.class));
    }

    private BtVodEntry row() {
        
        BtVodEntry entry = new BtVodEntry();
        entry.setGuid(PRODUCT_ID);
        entry.setId("12345");
        entry.setTitle(FULL_EPISODE_TITLE);
        entry.setProductOfferStartDate(1364774400000L); //"Apr  1 2013 12:00AM"
        entry.setProductOfferEndDate(1398816000000L);// "Apr 30 2014 12:00AM"
        entry.setProductType("episode");
        entry.setProductTags(ImmutableList.<BtVodPlproduct$productTag>of());
        entry.setProductScopes(ImmutableList.<BtVodProductScope>of());
        return entry;
    }
    

    private String brandUri() {
        return URI_PREFIX + "brands/" + PRODUCT_ID;
    }
}
