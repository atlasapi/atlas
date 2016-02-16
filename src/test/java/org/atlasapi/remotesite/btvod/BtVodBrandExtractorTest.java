package org.atlasapi.remotesite.btvod;

import java.util.Map;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.topic.TopicCreatingTopicResolver;
import org.atlasapi.persistence.topic.TopicWriter;
import org.atlasapi.remotesite.btvod.contentgroups.BtVodContentMatchingPredicates;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$productTag;
import org.atlasapi.remotesite.btvod.model.BtVodProductScope;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BtVodBrandExtractorTest {

    private static final String BRAND_TITLE = "Brand Title";
    private static final String PRODUCT_ID = "1234";
    private static final String REAL_EPISODE_TITLE = "Real Title";
    private static final String FULL_EPISODE_TITLE = BRAND_TITLE + ": S1 S1-E9 " + REAL_EPISODE_TITLE;
    private static final Publisher PUBLISHER = Publisher.BT_VOD;
    private static final String URI_PREFIX = "http://example.org/";
    private static final String BT_VOD_GUID_NAMESPACE = "guid namespace";
    private static final String BT_VOD_ID_NAMESPACE = "id namespace";
    private static final String BT_VOD_CONTENT_PROVIDER_NAMESPACE = "content provider namespace";
    private static final String BT_VOD_GENRE_NAMESPACE = "genre namespace";
    private static final String BT_VOD_CHANNEL_ID_NAMESPACE = "channel id namespace";

    private final BtVodContentListener contentListener = mock(BtVodContentListener.class);
    private final ImageUriProvider imageUriProvider = mock(ImageUriProvider.class);
    private final TopicCreatingTopicResolver topicResolver = mock(TopicCreatingTopicResolver.class);
    private final TopicWriter topicWriter = mock(TopicWriter.class);
    private final BrandUriExtractor brandUriExtractor = new BrandUriExtractor(URI_PREFIX, new TitleSanitiser());
    private final BtVodContentMatchingPredicate newTopicContentMatchingPredicate = mock(BtVodContentMatchingPredicate.class);
    private final BtVodTagMap btVodTagMap = mock(BtVodTagMap.class);

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
            BT_VOD_GUID_NAMESPACE,
            BT_VOD_ID_NAMESPACE,
            BT_VOD_CONTENT_PROVIDER_NAMESPACE,
            BT_VOD_GENRE_NAMESPACE,
            BT_VOD_CHANNEL_ID_NAMESPACE,
            btVodTagMap
    );

    private final BtVodBrandExtractor brandExtractor = new BtVodBrandExtractor(
            PUBLISHER,
            contentListener,
            Sets.<String>newHashSet(),
            describedFieldsExtractor,
            brandUriExtractor,
            btVodTagMap
    );
    
    @Test
    public void testCreatesSyntheticBrandFromEpisodeData() {
        BtVodEntry row = row(FULL_EPISODE_TITLE, PRODUCT_ID, "episode", "parentGuid");
        
        when(imageUriProvider.imageUriFor(Matchers.anyString())).thenReturn(Optional.<String>absent());

        brandExtractor.process(row);


        Brand extracted = Iterables.getOnlyElement(brandExtractor.getProcessedBrands().values());
        assertThat(extracted.getCanonicalUri(), is(URI_PREFIX + "synthesized/brands/brand-title"));
        assertThat(extracted.getTitle(), is(BRAND_TITLE));
    }
    
    @Test
    public void testCreatesSyntheticBrandFromEpisodeDataWithoutEpisodeTitle() {
        BtVodEntry row = row("Mad Men S01 E01", PRODUCT_ID, "episode", "parentGuid");
        
        when(imageUriProvider.imageUriFor(Matchers.anyString())).thenReturn(Optional.<String>absent());

        brandExtractor.process(row);


        Brand extracted = Iterables.getOnlyElement(brandExtractor.getProcessedBrands().values());
        assertThat(extracted.getCanonicalUri(), is(URI_PREFIX + "synthesized/brands/mad-men"));
        assertThat(extracted.getTitle(), is("Mad Men"));
    }


    @Test
    public void testDoesntCreateSyntheticBrandFromNonEpisodeData() {
        when(imageUriProvider.imageUriFor(Matchers.anyString())).thenReturn(Optional.<String>absent());

        BtVodEntry entry = row(FULL_EPISODE_TITLE, PRODUCT_ID, "episode", "parentGuid");
        entry.setProductType("film");

        brandExtractor.process(entry);

        assertThat(brandExtractor.getProcessedBrands().isEmpty(), is(true));

    }

    @Test
    public void testCreatesParentGuidToBrandMappingFromEpisode() {
        BtVodEntry row = row(FULL_EPISODE_TITLE, PRODUCT_ID, "episode", "parentGuid");

        when(imageUriProvider.imageUriFor(Matchers.anyString())).thenReturn(Optional.<String>absent());

        brandExtractor.process(row);

        Brand extracted = Iterables.getOnlyElement(brandExtractor.getProcessedBrands().values());

        assertThat(brandExtractor.getParentGuidToBrand().get(row.getParentGuid()),
                sameInstance(extracted));
    }

    @Test
    public void testUpdateParentGuidForAlreadyCreatedBrands() throws Exception {
        BtVodEntry season = row(BRAND_TITLE + " S5", "1", "season", "parentGuidSeason");
        BtVodEntry episode = row(FULL_EPISODE_TITLE, "2", "episode", "parentGuidEpisode");

        when(imageUriProvider.imageUriFor(Matchers.anyString())).thenReturn(Optional.<String>absent());

        brandExtractor.process(season);
        brandExtractor.process(episode);

        Brand extracted = Iterables.getOnlyElement(brandExtractor.getProcessedBrands().values());

        Map<String, Brand> parentGuidToBrand = brandExtractor.getParentGuidToBrand();
        assertThat(parentGuidToBrand.get(season.getParentGuid()), sameInstance(extracted));
        assertThat(parentGuidToBrand.get(episode.getParentGuid()), sameInstance(extracted));
    }

    private BtVodEntry row(String title, String guid, String productType, String parentGuid) {
        
        BtVodEntry entry = new BtVodEntry();
        entry.setGuid(guid);
        entry.setId("12345");
        entry.setParentGuid(parentGuid);
        entry.setTitle(title);
        entry.setProductOfferStartDate(1364774400000L); //"Apr  1 2013 12:00AM"
        entry.setProductOfferEndDate(1398816000000L);// "Apr 30 2014 12:00AM"
        entry.setProductType(productType);
        entry.setProductTags(ImmutableList.<BtVodPlproduct$productTag>of());
        entry.setProductScopes(ImmutableList.<BtVodProductScope>of());

        return entry;
    }
    

}
