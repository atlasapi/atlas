package org.atlasapi.remotesite.btvod;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Policy.RevenueContract;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.SeriesRef;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.topic.TopicCreatingTopicResolver;
import org.atlasapi.persistence.topic.TopicWriter;
import org.atlasapi.remotesite.btvod.contentgroups.BtVodContentMatchingPredicates;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$productTag;
import org.atlasapi.remotesite.btvod.model.BtVodProductMetadata;
import org.atlasapi.remotesite.btvod.model.BtVodProductPricingPlan;
import org.atlasapi.remotesite.btvod.model.BtVodProductRating;
import org.atlasapi.remotesite.btvod.model.BtVodProductScope;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Matchers;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;


public class BtVodItemExtractorTest {

    private static final String SUBSCRIPTION_CODE = "S012345";
    private static final String PRODUCT_GUID = "1234";
    private static final String PRODUCT_ID = "http://example.org/content/1244";
    private static final String SERIES_TITLE = "Series Title";
    private static final String REAL_EPISODE_TITLE = "Real Title";
    private static final String FULL_EPISODE_TITLE = SERIES_TITLE + ": S1 S1-E9 " + REAL_EPISODE_TITLE;
    private static final Publisher PUBLISHER = Publisher.BT_VOD;
    private static final String URI_PREFIX = "http://example.org/";
    private static final String SYNOPSIS = "Synopsis";
    private static final String BRAND_URI = URI_PREFIX + "brands/1234";
    private static final String TRAILER_URI = "http://vod.bt.com/trailer/1224";
    private static final Topic NEW_TOPIC = new Topic(123L);
    private static final String BT_VOD_GUID_NAMESPACE = "guid namespace";
    private static final String BT_VOD_ID_NAMESPACE = "id namespace";
    private static final String BT_VOD_CONTENT_PROVIDER_NAMESPACE = "content provider namespace";
    private static final String BT_VOD_GENRE_NAMESPACE = "genre namespace";

    private static final String BT_VOD_VERSION_GUID_NAMESPACE = "version:guid:namespace";
    private static final String BT_VOD_VERSION_ID_NAMESPACE = "version:id:namespace";

    private final BtVodBrandProvider btVodBrandProvider = mock(BtVodBrandProvider.class);
    private final BtVodSeriesProvider seriesProvider = mock(BtVodSeriesProvider.class);
    private final BtVodContentListener contentListener = mock(BtVodContentListener.class);
    private final ImageExtractor imageExtractor = mock(ImageExtractor.class);
    private final TopicCreatingTopicResolver topicResolver = mock(TopicCreatingTopicResolver.class);
    private final TopicWriter topicWriter = mock(TopicWriter.class);
    private final BtVodContentMatchingPredicate newTopicContentMatchingPredicate = mock(BtVodContentMatchingPredicate.class);
    private final TopicRef newTopicRef = new TopicRef(
            NEW_TOPIC,
            1.0f,
            false,
            TopicRef.Relationship.ABOUT
    );


    private final BtVodItemExtractor itemExtractor
            = new BtVodItemExtractor(
            btVodBrandProvider,
            seriesProvider,
            PUBLISHER, URI_PREFIX,
            contentListener,
            new BtVodDescribedFieldsExtractor(
                    topicResolver,
                    topicWriter,
                    Publisher.BT_VOD,
                    newTopicContentMatchingPredicate,
                    BtVodContentMatchingPredicates.schedulerChannelPredicate("Kids"),
                    BtVodContentMatchingPredicates.schedulerChannelAndOfferingTypePredicate(
                            "TV", ImmutableSet.of("Season", "Season-EST")
                    ),
                    BtVodContentMatchingPredicates.schedulerChannelPredicate("TV Replay"),
                    NEW_TOPIC,
                    new Topic(234L),
                    new Topic(345L),
                    new Topic(456L),
                    BT_VOD_GUID_NAMESPACE,
                    BT_VOD_ID_NAMESPACE,
                    BT_VOD_CONTENT_PROVIDER_NAMESPACE,
                    BT_VOD_GENRE_NAMESPACE
            ),
            Sets.<String>newHashSet(),
            new TitleSanitiser(),
            new NoImageExtractor(),
            new BtVodVersionsExtractor(
                    new BtVodPricingAvailabilityGrouper(),
                    URI_PREFIX,
                    BT_VOD_VERSION_GUID_NAMESPACE,
                    BT_VOD_VERSION_ID_NAMESPACE,
                    null,
                    null
            ),
            ImmutableSet.of(
                            newTopicRef,
                            new TopicRef(new Topic(234L), 1.0f, false, TopicRef.Relationship.ABOUT),
                            new TopicRef(new Topic(345L), 1.0f, false, TopicRef.Relationship.ABOUT),
                            new TopicRef(new Topic(456L), 1.0f, false, TopicRef.Relationship.ABOUT)
                    )
    );
    
    @Test
    public void testExtractsEpisode() {
        BtVodEntry btVodEntry = episodeRow(FULL_EPISODE_TITLE, PRODUCT_GUID);
        ParentRef parentRef = new ParentRef(BRAND_URI);
        Series series = new Series();
        series.setCanonicalUri("seriesUri");
        series.withSeriesNumber(1);

        when(seriesProvider.seriesFor(btVodEntry)).thenReturn(Optional.of(series));

        when(imageExtractor.imagesFor(Matchers.<BtVodEntry>any())).thenReturn(ImmutableSet.<Image>of());
        when(btVodBrandProvider.brandRefFor(btVodEntry)).thenReturn(Optional.of(parentRef));

        itemExtractor.process(btVodEntry);
        Item writtenItem = Iterables.getOnlyElement(itemExtractor.getProcessedItems().values());

        assertThat(writtenItem.getTitle(), is(REAL_EPISODE_TITLE));
        assertThat(writtenItem.getDescription(), is(SYNOPSIS));
        assertThat(writtenItem.getContainer(), is(parentRef));

        Location location = Iterables.getOnlyElement(
                                Iterables.getOnlyElement(
                                        Iterables.getOnlyElement(writtenItem.getVersions())
                                            .getManifestedAs())
                                            .getAvailableAt());

        DateTime expectedAvailabilityStart = new DateTime(2013, DateTimeConstants.APRIL, 1, 0, 0, 0, 0, DateTimeZone.UTC);
        DateTime expectedAvailabilityEnd = new DateTime(2014, DateTimeConstants.APRIL, 30, 0, 0, 0, 0, DateTimeZone.UTC);
        assertThat(location.getPolicy().getAvailabilityStart(), is(expectedAvailabilityStart));
        assertThat(location.getPolicy().getAvailabilityEnd(), is(expectedAvailabilityEnd));
        assertThat(location.getPolicy().getSubscriptionPackages(), is((Set<String>)ImmutableSet.of(SUBSCRIPTION_CODE)));
        assertThat(
                Iterables.getOnlyElement(writtenItem.getClips()),
                is(new Clip(TRAILER_URI, TRAILER_URI,Publisher.BT_VOD))
        );

        Set<Alias> expectedAliases =
                ImmutableSet.of(
                        new Alias(BT_VOD_GUID_NAMESPACE, btVodEntry.getGuid()),
                        new Alias(BT_VOD_ID_NAMESPACE, btVodEntry.getId())
                );


        assertThat(writtenItem.getAliases(), is(expectedAliases));
        assertThat(Iterables.getOnlyElement(location.getPolicy().getAvailableCountries()).code(), is("GB"));
        assertThat(location.getPolicy().getRevenueContract(), is(RevenueContract.SUBSCRIPTION));
    }

    @Test
    @Ignore
    // Ingored until we have real data which allows us to
    // correctly implement availability criteria
    public void testOnlyExtractsTrailerWhenMatchesCriteria() {
        BtVodEntry btVodEntry = episodeRow(FULL_EPISODE_TITLE, PRODUCT_GUID);
        ParentRef parentRef = new ParentRef(BRAND_URI);

        when(imageExtractor.imagesFor(Matchers.<BtVodEntry>any())).thenReturn(ImmutableSet.<Image>of());
        when(seriesProvider.seriesFor(btVodEntry)).thenReturn(Optional.of(mock(Series.class)));
        when(btVodBrandProvider.brandRefFor(btVodEntry)).thenReturn(Optional.of(parentRef));

        btVodEntry.setProductTags(ImmutableList.<BtVodPlproduct$productTag>of());

        Item writtenItem = Iterables.getOnlyElement(itemExtractor.getProcessedItems().values());

        assertTrue(writtenItem.getClips().isEmpty());

    }


    @Test
    public void testMergesVersionsForHDandSD() {
        BtVodEntry btVodEntrySD = episodeRow(FULL_EPISODE_TITLE, PRODUCT_GUID);
        ParentRef parentRef = new ParentRef(BRAND_URI);
        Series series = new Series();
        series.setCanonicalUri("seriesUri");
        series.withSeriesNumber(1);


        BtVodEntry btVodEntryHD = episodeRow(FULL_EPISODE_TITLE, PRODUCT_GUID);
        btVodEntryHD.setTitle(FULL_EPISODE_TITLE + " - HD");
        btVodEntryHD.setGuid(PRODUCT_GUID + "_HD");

        when(seriesProvider.seriesFor(btVodEntrySD)).thenReturn(Optional.of(series));
        when(seriesProvider.seriesFor(btVodEntryHD)).thenReturn(Optional.of(series));

        when(imageExtractor.imagesFor(Matchers.<BtVodEntry>any())).thenReturn(ImmutableSet.<Image>of());
        when(btVodBrandProvider.brandRefFor(btVodEntrySD)).thenReturn(Optional.of(parentRef));
        when(btVodBrandProvider.brandRefFor(btVodEntryHD)).thenReturn(Optional.of(parentRef));

        itemExtractor.process(btVodEntrySD);
        itemExtractor.process(btVodEntryHD);

        Item writtenItem = Iterables.getOnlyElement(itemExtractor.getProcessedItems().values());

        assertThat(writtenItem.getTitle(), is(REAL_EPISODE_TITLE));
        assertThat(writtenItem.getDescription(), is(SYNOPSIS));
        assertThat(writtenItem.getContainer(), is(parentRef));

        assertThat(writtenItem.getVersions().size(), is(2));
        assertThat(writtenItem.getClips().size(), is(2));
                
        

    }

    @Test
    public void testMergesVersionsForHDandSDForEpisodes() {
        testHdSdMerging(SERIES_TITLE + ": - HD S1 S1-E9 " + REAL_EPISODE_TITLE + " - HD", FULL_EPISODE_TITLE, REAL_EPISODE_TITLE);
    }
    
    @Test 
    public void testMergesVersionsForCollections() {
        testHdSdMerging("Ben & Holly's Elf and Fairy Party - HD - Lucy's Sleepover - HD", "Ben & Holly's Elf and Fairy Party - Lucy's Sleepover", "Lucy's Sleepover");
    }
    
    private void testHdSdMerging(String hdTitle, String sdTitle, String extractedTitle) {
        BtVodEntry btVodEntrySD = episodeRow(FULL_EPISODE_TITLE, PRODUCT_GUID);
        btVodEntrySD.setTitle(sdTitle);
        ParentRef parentRef = new ParentRef(BRAND_URI);
        btVodEntrySD.setProductTargetBandwidth("SD");

        BtVodEntry btVodEntryHD = episodeRow(FULL_EPISODE_TITLE, PRODUCT_GUID);
        btVodEntryHD.setTitle(hdTitle);
        btVodEntryHD.setGuid(PRODUCT_GUID + "_HD");
        btVodEntryHD.setProductTargetBandwidth("HD");

        Series series = new Series();
        series.setCanonicalUri("seriesUri");
        series.withSeriesNumber(1);

        when(seriesProvider.seriesFor(btVodEntrySD)).thenReturn(Optional.of(series));
        when(seriesProvider.seriesFor(btVodEntryHD)).thenReturn(Optional.of(series));

        when(imageExtractor.imagesFor(Matchers.<BtVodEntry>any())).thenReturn(ImmutableSet.<Image>of());
        when(btVodBrandProvider.brandRefFor(btVodEntrySD)).thenReturn(Optional.of(parentRef));
        when(btVodBrandProvider.brandRefFor(btVodEntryHD)).thenReturn(Optional.of(parentRef));

        itemExtractor.process(btVodEntrySD);
        itemExtractor.process(btVodEntryHD);

        Item writtenItem = Iterables.getOnlyElement(itemExtractor.getProcessedItems().values());

        assertThat(writtenItem.getTitle(), is(extractedTitle));
        assertThat(writtenItem.getDescription(), is(SYNOPSIS));
        assertThat(writtenItem.getContainer(), is(parentRef));

        assertThat(writtenItem.getVersions().size(), is(2));

        Version hdVersion = Iterables.get(writtenItem.getVersions(), 0);
        Version sdVersion = Iterables.get(writtenItem.getVersions(), 1);
        assertThat(Iterables.getOnlyElement(sdVersion.getManifestedAs()).getHighDefinition(), is(false));
        assertThat(Iterables.getOnlyElement(hdVersion.getManifestedAs()).getHighDefinition(), is(true));
        assertThat(
                Iterables.getFirst(writtenItem.getClips(), null),
                is(new Clip(TRAILER_URI, TRAILER_URI,Publisher.BT_VOD))
        );
    }



    @Test
    public void testMergesHDandSDforFilms() {
        BtVodEntry btVodEntrySD = filmRow("About Alex");
        btVodEntrySD.setProductTargetBandwidth("SD");

        BtVodEntry btVodEntryHD = filmRow("About Alex - HD");
        btVodEntryHD.setGuid(PRODUCT_GUID + "_HD");
        btVodEntryHD.setProductTargetBandwidth("HD");


        when(imageExtractor.imagesFor(Matchers.<BtVodEntry>any())).thenReturn(ImmutableSet.<Image>of());

        when(btVodBrandProvider.brandRefFor(btVodEntrySD)).thenReturn(Optional.<ParentRef>absent());
        when(btVodBrandProvider.brandRefFor(btVodEntryHD)).thenReturn(Optional.<ParentRef>absent());
        
        when(seriesProvider.seriesFor(btVodEntryHD)).thenReturn(Optional.<Series>absent());
        when(seriesProvider.seriesFor(btVodEntrySD)).thenReturn(Optional.<Series>absent());

        itemExtractor.process(btVodEntrySD);
        itemExtractor.process(btVodEntryHD);

        Item writtenItem = Iterables.getOnlyElement(itemExtractor.getProcessedItems().values());

        assertThat(writtenItem.getTitle(), is("About Alex"));
        assertThat(writtenItem.getDescription(), is(SYNOPSIS));

        assertThat(writtenItem.getVersions().size(), is(2));

        Version hdVersion = Iterables.get(writtenItem.getVersions(), 0);
        Version sdVersion = Iterables.get(writtenItem.getVersions(), 1);
        assertThat(Iterables.getOnlyElement(sdVersion.getManifestedAs()).getHighDefinition(), is(false));
        assertThat(Iterables.getOnlyElement(hdVersion.getManifestedAs()).getHighDefinition(), is(true));
    }

    @Test
    public void testMergesFilmsFromCurzon() {
        BtVodEntry btVodEntrySD = filmRow("Amour");

        BtVodEntry btVodEntryHD = filmRow("Amour (Curzon)");
        btVodEntryHD.setGuid(PRODUCT_GUID + "Curzon");

        BtVodEntry btVodEntryHDCurzon = filmRow("Amour (Curzon) - HD");
        btVodEntryHDCurzon.setGuid(PRODUCT_GUID + "Curzon_HD");

        when(imageExtractor.imagesFor(Matchers.<BtVodEntry>any())).thenReturn(ImmutableSet.<Image>of());

        when(btVodBrandProvider.brandRefFor(btVodEntrySD)).thenReturn(Optional.<ParentRef>absent());
        when(btVodBrandProvider.brandRefFor(btVodEntryHD)).thenReturn(Optional.<ParentRef>absent());
        when(btVodBrandProvider.brandRefFor(btVodEntryHDCurzon)).thenReturn(Optional.<ParentRef>absent());

        when(seriesProvider.seriesFor(btVodEntrySD)).thenReturn(Optional.<Series>absent());
        when(seriesProvider.seriesFor(btVodEntryHD)).thenReturn(Optional.<Series>absent());
        when(seriesProvider.seriesFor(btVodEntryHDCurzon)).thenReturn(Optional.<Series>absent());

        itemExtractor.process(btVodEntrySD);
        itemExtractor.process(btVodEntryHD);
        itemExtractor.process(btVodEntryHDCurzon);

        Item writtenItem = Iterables.getOnlyElement(itemExtractor.getProcessedItems().values());

        assertThat(writtenItem.getTitle(), is("Amour"));
        assertThat(writtenItem.getDescription(), is(SYNOPSIS));

        assertThat(writtenItem.getVersions().size(), is(3));

    }
    
    @Test
    public void testExtractsItem() {
        
    }
    
    private BtVodEntry episodeRow(String title, String guid) {
        BtVodEntry entry = new BtVodEntry();
        entry.setGuid(guid);
        entry.setId(PRODUCT_ID);
        entry.setTitle(title);
        entry.setProductOfferStartDate(1364774400000L); //"Apr  1 2013 12:00AM"
        entry.setProductOfferEndDate(1398816000000L);// "Apr 30 2014 12:00AM"
        entry.setDescription(SYNOPSIS);
        entry.setProductType("episode");
        entry.setProductPricingPlan(new BtVodProductPricingPlan());
        entry.setProductTrailerMediaId(TRAILER_URI);
        BtVodProductScope productScope = new BtVodProductScope();
        BtVodProductMetadata productMetadata = new BtVodProductMetadata();
        productMetadata.setEpisodeNumber("1");
        productScope.setProductMetadata(productMetadata);
        entry.setProductScopes(ImmutableList.of(productScope));
        entry.setProductRatings(ImmutableList.<BtVodProductRating>of());
        BtVodPlproduct$productTag tag = new BtVodPlproduct$productTag();
        tag.setPlproduct$scheme("subscription");
        tag.setPlproduct$title(SUBSCRIPTION_CODE);

        BtVodPlproduct$productTag trailerCdnAvailabilityTag = new BtVodPlproduct$productTag();
        trailerCdnAvailabilityTag.setPlproduct$scheme("trailerServiceType");
        trailerCdnAvailabilityTag.setPlproduct$title("OTG");

        BtVodPlproduct$productTag itemCdnAvailabilityTag = new BtVodPlproduct$productTag();
        itemCdnAvailabilityTag.setPlproduct$scheme("serviceType");
        itemCdnAvailabilityTag.setPlproduct$title("OTG");
        
        BtVodPlproduct$productTag itemMasterAgreementAvailabilityTag = new BtVodPlproduct$productTag();
        itemMasterAgreementAvailabilityTag.setPlproduct$scheme("masterAgreementOtgTvodPlay");
        itemMasterAgreementAvailabilityTag.setPlproduct$title("TRUE");        

        entry.setProductTags(ImmutableList.of(tag,
                trailerCdnAvailabilityTag,
                itemCdnAvailabilityTag,
                itemMasterAgreementAvailabilityTag));


        return entry;
    }

    private BtVodEntry filmRow(String title) {
        BtVodEntry entry = new BtVodEntry();
        entry.setGuid(PRODUCT_GUID);
        entry.setId(PRODUCT_ID);
        entry.setTitle(title);
        entry.setProductOfferStartDate(new DateTime(2013, DateTimeConstants.APRIL, 1, 0, 0, 0, 0).getMillis());
        entry.setProductOfferEndDate(new DateTime(2014, DateTimeConstants.APRIL, 30, 0, 0, 0).getMillis());
        entry.setDescription(SYNOPSIS);
        entry.setProductType("film");
        entry.setProductPricingPlan(new BtVodProductPricingPlan());
        entry.setProductTrailerMediaId(TRAILER_URI);
        BtVodProductScope productScope = new BtVodProductScope();
        BtVodProductMetadata productMetadata = new BtVodProductMetadata();
        productMetadata.setReleaseYear("2015");
        productScope.setProductMetadata(productMetadata);
        entry.setProductScopes(ImmutableList.of(productScope));
        entry.setProductRatings(ImmutableList.<BtVodProductRating>of());
        BtVodPlproduct$productTag tag = new BtVodPlproduct$productTag();
        tag.setPlproduct$scheme("subscription");
        tag.setPlproduct$title(SUBSCRIPTION_CODE);

        BtVodPlproduct$productTag itemCdnAvailabilityTag = new BtVodPlproduct$productTag();
        itemCdnAvailabilityTag.setPlproduct$scheme("serviceType");
        itemCdnAvailabilityTag.setPlproduct$title("OTG");
        
        BtVodPlproduct$productTag itemMasterAgreementAvailabilityTag = new BtVodPlproduct$productTag();
        itemMasterAgreementAvailabilityTag.setPlproduct$scheme("masterAgreementOtgTvodPlay");
        itemMasterAgreementAvailabilityTag.setPlproduct$title("TRUE"); 
        
        entry.setProductTags(ImmutableList.of(tag, itemCdnAvailabilityTag, itemMasterAgreementAvailabilityTag));



        return entry;
    }

    @Test
    public void testPropagatesNewTagToBrandAndSeries() {
        BtVodEntry btVodEntry = episodeRow(FULL_EPISODE_TITLE, PRODUCT_GUID);
        ParentRef parentRef = new ParentRef(BRAND_URI);
        Series series = mock(Series.class);
        Brand brand = mock(Brand.class);
        when(series.getCanonicalUri()).thenReturn("seriesUri");
        when(series.getSeriesNumber()).thenReturn(1);

        when(seriesProvider.seriesFor(btVodEntry)).thenReturn(Optional.of(series));
        when(btVodBrandProvider.brandFor(btVodEntry)).thenReturn(Optional.of(brand));

        when(imageExtractor.imagesFor(Matchers.<BtVodEntry>any())).thenReturn(ImmutableSet.<Image>of());
        when(btVodBrandProvider.brandRefFor(btVodEntry)).thenReturn(Optional.of(parentRef));
        when(newTopicContentMatchingPredicate.apply(isA(VodEntryAndContent.class))).thenReturn(true);

        itemExtractor.process(btVodEntry);

        Item writtenItem = Iterables.getOnlyElement(itemExtractor.getProcessedItems().values());

        assertThat(writtenItem.getTopicRefs().contains(newTopicRef), is(true));
        verify(series).addTopicRef(newTopicRef);
        verify(brand).addTopicRef(newTopicRef);
    }
    
    @Test
    public void testDoesntMergeEpisodesWithSameTitleAcrossDifferentBrands() {
        BtVodEntry btVodEntry = episodeRow(SERIES_TITLE + ": S1 S1-E9 " + REAL_EPISODE_TITLE, PRODUCT_GUID);
        BtVodEntry btVodEntryFromDifferentSeries = episodeRow("A different " + SERIES_TITLE + ": S1 S1-E9 " + REAL_EPISODE_TITLE, "99999");
        
        ParentRef parentRef = new ParentRef(BRAND_URI);
        ParentRef parentRefDiffSeries = new ParentRef(URI_PREFIX + "brands/a-different-1234");
        
        Series series = new Series();
        series.setCanonicalUri("seriesUri");
        series.withSeriesNumber(1);

        when(seriesProvider.seriesFor(btVodEntry)).thenReturn(Optional.of(series));

        
        when(imageExtractor.imagesFor(Matchers.<BtVodEntry>any())).thenReturn(ImmutableSet.<Image>of());

        
        when(btVodBrandProvider.brandRefFor(btVodEntry)).thenReturn(Optional.of(parentRef));
        when(btVodBrandProvider.brandRefFor(btVodEntryFromDifferentSeries)).thenReturn(Optional.of(parentRefDiffSeries));
        when(seriesProvider.seriesFor(btVodEntry)).thenReturn(Optional.of(series));
        when(seriesProvider.seriesFor(btVodEntryFromDifferentSeries)).thenReturn(Optional.of(series));

        itemExtractor.process(btVodEntry);
        itemExtractor.process(btVodEntryFromDifferentSeries);
        
        assertThat(itemExtractor.getProcessedItems().size(), is(2));

    }
}
