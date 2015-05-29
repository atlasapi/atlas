package org.atlasapi.remotesite.btvod;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodPlproductImages;
import org.atlasapi.remotesite.btvod.model.BtVodProductPricingPlan;
import org.atlasapi.remotesite.btvod.model.BtVodProductMetadata;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$productTag;
import org.atlasapi.remotesite.btvod.model.BtVodProductRating;
import org.atlasapi.remotesite.btvod.model.BtVodProductScope;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.mockito.Matchers;


public class BtVodItemWriterTest {

    private static final String IMAGE_URI = "http://example.org/123.png";
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

    private final ContentWriter contentWriter = mock(ContentWriter.class);
    private final ContentResolver contentResolver = mock(ContentResolver.class);
    private final BtVodBrandWriter brandExtractor = mock(BtVodBrandWriter.class);
    private final BtVodSeriesWriter seriesExtractor = mock(BtVodSeriesWriter.class);
    private final BtVodContentListener contentListener = mock(BtVodContentListener.class);
    private final ImageExtractor imageExtractor = mock(ImageExtractor.class);
    
    private final BtVodItemWriter itemExtractor 
                    = new BtVodItemWriter(
                                contentWriter, 
                                contentResolver, 
                                brandExtractor,
                                seriesExtractor,
                                PUBLISHER, URI_PREFIX,
                                contentListener,
                                new BtVodDescribedFieldsExtractor(imageExtractor),
                                Sets.<String>newHashSet(), new TitleSanitiser());
    
    @Test
    public void testExtractsEpisode() {
        BtVodEntry btVodEntry = episodeRow();
        ParentRef parentRef = new ParentRef(BRAND_URI);
        ParentRef seriesRef = new ParentRef("seriesUri");

        when(contentResolver.findByCanonicalUris(ImmutableSet.of(itemUri())))
                .thenReturn(ResolvedContent.builder().build());
        when(imageExtractor.extractImages(Matchers.<BtVodPlproductImages>any())).thenReturn(ImmutableSet.<Image>of());
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
        assertThat(
                Iterables.getOnlyElement(writtenItem.getClips()),
                is(new Clip(TRAILER_URI, TRAILER_URI,Publisher.BT_VOD))
        );
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
        btVodEntryHD.setGuid(PRODUCT_GUID + "_HD");

        when(contentResolver.findByCanonicalUris(ImmutableSet.of(itemUri())))
                .thenReturn(ResolvedContent.builder().build());
        when(imageExtractor.extractImages(Matchers.<BtVodPlproductImages>any())).thenReturn(ImmutableSet.<Image>of());
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
        assertThat(
                Iterables.getOnlyElement(writtenItem.getClips()),
                is(new Clip(TRAILER_URI, TRAILER_URI,Publisher.BT_VOD))
        );

    }

    @Test
    public void testMergesVersionsForHDandSDForEpisodes() {
        BtVodEntry btVodEntrySD = episodeRow();
        ParentRef parentRef = new ParentRef(BRAND_URI);
        ParentRef seriesRef = new ParentRef("seriesUri");
        btVodEntrySD.setProductTargetBandwidth("SD");

        BtVodEntry btVodEntryHD = episodeRow();
        btVodEntryHD.setTitle(SERIES_TITLE + ": - HD S1 S1-E9 " + REAL_EPISODE_TITLE + " - HD");
        btVodEntryHD.setGuid(PRODUCT_GUID + "_HD");
        btVodEntryHD.setProductTargetBandwidth("HD");

        when(contentResolver.findByCanonicalUris(ImmutableSet.of(itemUri())))
                .thenReturn(ResolvedContent.builder().build());
        when(imageExtractor.extractImages(Matchers.<BtVodPlproductImages>any())).thenReturn(ImmutableSet.<Image>of());
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
        assertThat(Iterables.getOnlyElement(sdVersion.getManifestedAs()).getHighDefinition(), is(false));
        assertThat(Iterables.getOnlyElement(hdVersion.getManifestedAs()).getHighDefinition(), is(true));
        assertThat(
                Iterables.getOnlyElement(writtenItem.getClips()),
                is(new Clip(TRAILER_URI, TRAILER_URI,Publisher.BT_VOD))
        );

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
        btVodEntry8.setTitle("ZQWModern_Family: S01 S1-E4 ZQWThe_Incident");

        BtVodEntry btVodEntry9 = new BtVodEntry();
        btVodEntry9.setTitle("ZQZPeppa_Pig: S01 S1-E4 ZQZSchool Play");

        BtVodEntry btVodEntry10 = new BtVodEntry();
        btVodEntry10.setTitle("ZQWAmerican_Horror_Story: S01 S1-E11 ZQWBirth");

        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry1.getTitle()), is(REAL_EPISODE_TITLE));
        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry2.getTitle()), is("Conference Call"));
        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry3.getTitle()), is("Saracens v Leicester Tigers 2010/11"));
        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry4.getTitle()), is("1958 Sweden - Hinein!"));
        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry5.getTitle()), is("1958 Sweden - Hinein!"));
        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry6.getTitle()), is("Episode 2"));
        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry7.getTitle()), is("Truth Be Told"));
        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry8.getTitle()), is("The Incident"));
        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry9.getTitle()), is("School Play"));
        assertThat(itemExtractor.extractEpisodeTitle(btVodEntry10.getTitle()), is("Birth"));
    }
    
    @Test
    public void testExtractsFilm() {
        
    }
    
    @Test
    public void testExtractsItem() {
        
    }
    
    private BtVodEntry episodeRow() {
        BtVodEntry entry = new BtVodEntry();
        entry.setGuid(PRODUCT_GUID);
        entry.setId(PRODUCT_ID);
        entry.setTitle(FULL_EPISODE_TITLE);
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
        entry.setProductTags(ImmutableList.<BtVodPlproduct$productTag>of());

        return entry;
    }
    
    private String itemUri() {
        return URI_PREFIX + "items/" + PRODUCT_GUID;
    }
}
