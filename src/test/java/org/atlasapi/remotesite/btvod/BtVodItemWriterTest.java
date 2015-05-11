package org.atlasapi.remotesite.btvod;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$pricingPlan;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$productMetadata;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$productTag;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$ratings;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$scopes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;


public class BtVodItemWriterTest {

    private static final String IMAGE_URI = "http://example.org/123.png";
    private static final String PRODUCT_ID = "1234";
    private static final String SERIES_TITLE = "Series Title";
    private static final String REAL_EPISODE_TITLE = "Real Title";
    private static final String FULL_EPISODE_TITLE = SERIES_TITLE + ": S1 S1-E9 " + REAL_EPISODE_TITLE;
    private static final Publisher PUBLISHER = Publisher.BT_VOD;
    private static final String URI_PREFIX = "http://example.org/";
    private static final String SYNOPSIS = "Synopsis";
    private static final String BRAND_URI = URI_PREFIX + "brands/1234";
    
    private final ContentWriter contentWriter = mock(ContentWriter.class);
    private final ContentResolver contentResolver = mock(ContentResolver.class);
    private final BtVodBrandWriter brandExtractor = mock(BtVodBrandWriter.class);
    private final BtVodSeriesWriter seriesExtractor = mock(BtVodSeriesWriter.class);
    private final BtVodContentListener contentListener = mock(BtVodContentListener.class);
    private final ImageUriProvider imageUriProvider = mock(ImageUriProvider.class);
    
    private final BtVodItemWriter itemExtractor 
                    = new BtVodItemWriter(
                                contentWriter, 
                                contentResolver, 
                                brandExtractor,
                                seriesExtractor,
                                PUBLISHER, URI_PREFIX,
                                contentListener,
                                new BtVodDescribedFieldsExtractor(imageUriProvider),
                                Sets.<String>newHashSet(), new TitleSanitiser());
    
    @Test
    public void testExtractsEpisode() {
        BtVodEntry btVodEntry = episodeRow();
        ParentRef parentRef = new ParentRef(BRAND_URI);
        ParentRef seriesRef = new ParentRef("seriesUri");

        when(contentResolver.findByCanonicalUris(ImmutableSet.of(itemUri())))
                .thenReturn(ResolvedContent.builder().build());
        when(imageUriProvider.imageUriFor(PRODUCT_ID)).thenReturn(Optional.<String>of(IMAGE_URI));
        when(seriesExtractor.getSeriesRefFor(btVodEntry)).thenReturn(Optional.of(seriesRef));
        when(seriesExtractor.extractSeriesNumber(btVodEntry.getTitle())).thenReturn(Optional.of(1));
        when(brandExtractor.getBrandRefFor(btVodEntry)).thenReturn(Optional.of(parentRef));

        itemExtractor.process(btVodEntry);
        
        ArgumentCaptor<Item> itemCaptor = ArgumentCaptor.forClass(Item.class);
        verify(contentWriter).createOrUpdate(itemCaptor.capture());

        Item writtenItem = itemCaptor.getValue();
        
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
        //assertThat(Iterables.getOnlyElement(location.getPolicy().getAvailableCountries()).code(), is("GB"));
        //assertThat(location.getPolicy().getRevenueContract(), is(RevenueContract.PAY_TO_RENT));
    }

    @Test
    public void testMergesVersionsForHDandSD() {
        BtVodEntry btVodEntrySD = episodeRow();
        ParentRef parentRef = new ParentRef(BRAND_URI);
        ParentRef seriesRef = new ParentRef("seriesUri");

        BtVodEntry btVodEntryHD = episodeRow();
        btVodEntryHD.setTitle(FULL_EPISODE_TITLE + " - HD");
        btVodEntryHD.setGuid(PRODUCT_ID + "_HD");

        when(contentResolver.findByCanonicalUris(ImmutableSet.of(itemUri())))
                .thenReturn(ResolvedContent.builder().build());
        when(imageUriProvider.imageUriFor(PRODUCT_ID)).thenReturn(Optional.<String>of(IMAGE_URI));
        when(seriesExtractor.getSeriesRefFor(btVodEntrySD)).thenReturn(Optional.of(seriesRef));
        when(seriesExtractor.extractSeriesNumber(btVodEntrySD.getTitle())).thenReturn(Optional.of(1));
        when(brandExtractor.getBrandRefFor(btVodEntrySD)).thenReturn(Optional.of(parentRef));

        itemExtractor.process(btVodEntrySD);
        itemExtractor.process(btVodEntryHD);

        ArgumentCaptor<Item> itemCaptor = ArgumentCaptor.forClass(Item.class);
        verify(contentWriter, times(2)).createOrUpdate(itemCaptor.capture());


        Item writtenItem = Iterables.getOnlyElement(ImmutableSet.copyOf(itemCaptor.getAllValues()));

        assertThat(writtenItem.getTitle(), is(REAL_EPISODE_TITLE));
        assertThat(writtenItem.getDescription(), is(SYNOPSIS));
        assertThat(writtenItem.getContainer(), is(parentRef));

        assertThat(writtenItem.getVersions().size(), is(2));

    }

    @Test
    public void testMergesVersionsForHDandSDForEpisodes() {
        BtVodEntry btVodEntrySD = episodeRow();
        ParentRef parentRef = new ParentRef(BRAND_URI);
        ParentRef seriesRef = new ParentRef("seriesUri");
        btVodEntrySD.setBtproduct$targetBandwidth("SD");

        BtVodEntry btVodEntryHD = episodeRow();
        btVodEntryHD.setTitle(SERIES_TITLE + ": - HD S1 S1-E9 " + REAL_EPISODE_TITLE + " - HD");
        btVodEntryHD.setGuid(PRODUCT_ID + "_HD");
        btVodEntryHD.setBtproduct$targetBandwidth("HD");

        when(contentResolver.findByCanonicalUris(ImmutableSet.of(itemUri())))
                .thenReturn(ResolvedContent.builder().build());
        when(imageUriProvider.imageUriFor(PRODUCT_ID)).thenReturn(Optional.<String>of(IMAGE_URI));
        when(seriesExtractor.getSeriesRefFor(btVodEntrySD)).thenReturn(Optional.of(seriesRef));
        when(seriesExtractor.extractSeriesNumber(btVodEntrySD.getTitle())).thenReturn(Optional.of(1));
        when(brandExtractor.getBrandRefFor(btVodEntrySD)).thenReturn(Optional.of(parentRef));

        itemExtractor.process(btVodEntrySD);
        itemExtractor.process(btVodEntryHD);

        ArgumentCaptor<Item> itemCaptor = ArgumentCaptor.forClass(Item.class);
        verify(contentWriter, times(2)).createOrUpdate(itemCaptor.capture());


        Item writtenItem = Iterables.getOnlyElement(ImmutableSet.copyOf(itemCaptor.getAllValues()));

        assertThat(writtenItem.getTitle(), is(REAL_EPISODE_TITLE));
        assertThat(writtenItem.getDescription(), is(SYNOPSIS));
        assertThat(writtenItem.getContainer(), is(parentRef));

        assertThat(writtenItem.getVersions().size(), is(2));

        Version hdVersion = Iterables.get(writtenItem.getVersions(), 0);
        Version sdVersion = Iterables.get(writtenItem.getVersions(), 1);
        assertThat(Iterables.getOnlyElement(sdVersion.getManifestedAs()).getHd(), is(false));
        assertThat(Iterables.getOnlyElement(hdVersion.getManifestedAs()).getHd(), is(true));

    }

    @Test
    public void testExtractsEpisodeTitles() {
        BtVodEntry btVodEntry1 = episodeRow();
        btVodEntry1.setTitle(FULL_EPISODE_TITLE);

        BtVodEntry btVodEntry2 = episodeRow();
        btVodEntry2.setTitle("Cashmere Mafia S1-E2 Conference Call");

        BtVodEntry btVodEntry3 = episodeRow();
        btVodEntry3.setTitle("Classic Premiership Rugby - Saracens v Leicester Tigers 2010/11");

        BtVodEntry btVodEntry4 = episodeRow();
        btVodEntry4.setTitle("FIFA Films - 1958 Sweden - Hinein! - HD");

        BtVodEntry btVodEntry5 = episodeRow();
        btVodEntry5.setTitle("FIFA Films - 1958 Sweden - Hinein!");

        BtVodEntry btVodEntry6 = episodeRow();
        btVodEntry6.setTitle("UFC: The Ultimate Fighter Season 19 - Season 19 Episode 2");

        BtVodEntry btVodEntry7 = new BtVodEntry();
        btVodEntry7.setTitle("Modern Family: S03 - HD S3-E17 Truth Be Told - HD");

        BtVodEntry btVodEntry8 = new BtVodEntry();
        btVodEntry8.setTitle("ZQWModern_Family: S01 S1-E4 ZQWThe Incident");

        BtVodEntry btVodEntry9 = new BtVodEntry();
        btVodEntry9.setTitle("ZQZPeppa_Pig: S01 S1-E4 ZQZSchool Play");

        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry1.getTitle()), is(REAL_EPISODE_TITLE));
        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry2.getTitle()), is("Conference Call"));
        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry3.getTitle()), is("Saracens v Leicester Tigers 2010/11"));
        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry4.getTitle()), is("1958 Sweden - Hinein!"));
        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry5.getTitle()), is("1958 Sweden - Hinein!"));
        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry6.getTitle()), is("Episode 2"));
        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry7.getTitle()), is("Truth Be Told"));
        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry8.getTitle()), is("The Incident"));
        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry9.getTitle()), is("School Play"));
    }
    
    @Test
    public void testExtractsFilm() {
        
    }
    
    @Test
    public void testExtractsItem() {
        
    }
    
    private BtVodEntry episodeRow() {
        BtVodEntry entry = new BtVodEntry();
        entry.setGuid(PRODUCT_ID);
        entry.setTitle(FULL_EPISODE_TITLE);
        entry.setPlproduct$offerStartDate(1364774400000L); //"Apr  1 2013 12:00AM"
        entry.setPlproduct$offerEndDate(1398816000000L);// "Apr 30 2014 12:00AM"
        entry.setDescription(SYNOPSIS);
        entry.setBtproduct$productType("episode");
        entry.setPlproduct$pricingPlan(new BtVodPlproduct$pricingPlan());
        BtVodPlproduct$scopes productScope = new BtVodPlproduct$scopes();
        BtVodPlproduct$productMetadata productMetadata = new BtVodPlproduct$productMetadata();
        productMetadata.setEpisodeNumber("1");
        productScope.setPlproduct$productMetadata(productMetadata);
        entry.setPlproduct$scopes(ImmutableList.of(productScope));
        entry.setPlproduct$ratings(ImmutableList.<BtVodPlproduct$ratings>of());
        entry.setPlproduct$productTags(ImmutableList.<BtVodPlproduct$productTag>of());

        return entry;
    }
    
    private String itemUri() {
        return URI_PREFIX + "items/" + PRODUCT_ID;
    }
}
