package org.atlasapi.remotesite.btvod;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Policy.RevenueContract;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.remotesite.btvod.BtVodData.BtVodDataRow;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ImmutableMap.Builder;


public class BtVodItemExtractorTest {

    private static final String IMAGE_FILENAME = "image.png";
    private static final String PRODUCT_ID = "1234";
    private static final String EPISODE_TITLE = "Title";
    private static final Publisher PUBLISHER = Publisher.BT_VOD;
    private static final String URI_PREFIX = "http://example.org/";
    private static final String SYNOPSIS = "Synopsis";
    
    private final ContentWriter contentWriter = mock(ContentWriter.class);
    private final ContentResolver contentResolver = mock(ContentResolver.class);
    private final BtVodBrandExtractor brandExtractor = mock(BtVodBrandExtractor.class);
    private final BtVodSeriesExtractor seriesExtractor = mock(BtVodSeriesExtractor.class);
    private final BtVodContentListener contentListener = mock(BtVodContentListener.class);
    
    private final BtVodItemExtractor itemExtractor 
                    = new BtVodItemExtractor(
                                contentWriter, 
                                contentResolver, 
                                brandExtractor,
                                seriesExtractor,
                                PUBLISHER, URI_PREFIX,
                                contentListener);
    
    
    public void testExtractsEpisode() {
        when(contentResolver.findByCanonicalUris(ImmutableSet.of(itemUri())))
                .thenReturn(ResolvedContent.builder().build());
        
        itemExtractor.process(itemRow());
        
        ArgumentCaptor<Item> itemCaptor = ArgumentCaptor.forClass(Item.class);
        verify(contentWriter).createOrUpdate(itemCaptor.capture());
        
        Item writtenItem = itemCaptor.getValue();
        
        assertThat(writtenItem.getTitle(), is(EPISODE_TITLE));
        assertThat(writtenItem.getDescription(), is(SYNOPSIS));
        
        Image image = Iterables.getOnlyElement(writtenItem.getImages());
        assertThat(image.getCanonicalUri(), is("http://portal.vision.bt.com/btvo/" + IMAGE_FILENAME));
        assertThat(image.getType(), is(ImageType.PRIMARY));
        
        Location location = Iterables.getOnlyElement(
                                Iterables.getOnlyElement(
                                        Iterables.getOnlyElement(writtenItem.getVersions())
                                            .getManifestedAs())
                                            .getAvailableAt());
        
        DateTime expectedAvailabilityStart = new DateTime(2013, DateTimeConstants.MARCH, 31, 23, 0, 0, 0, DateTimeZone.UTC);
        DateTime expectedAvailabilityEnd = new DateTime(2014, DateTimeConstants.APRIL, 30, 0, 0, 0).withZone(DateTimeZone.UTC);
        assertThat(location.getPolicy().getAvailabilityStart(), is(expectedAvailabilityStart));
        assertThat(location.getPolicy().getAvailabilityEnd(), is(expectedAvailabilityEnd));
        //TODO assert duration 
        //TODO assert published duration
        assertThat(Iterables.getOnlyElement(location.getPolicy().getAvailableCountries()).code(), is("GB"));
        assertThat(location.getPolicy().getRevenueContract(), is(RevenueContract.PAY_TO_RENT));
    }
    
    @Test
    public void testExtractsFilm() {
        
    }
    
    @Test
    public void testExtractsItem() {
        
    }
    
    private BtVodDataRow itemRow() {
        Builder<String, String> rows = ImmutableMap.builder();
        rows.put(BtVodFileColumn.BRANDIA_ID.key(), "");
        rows.put(BtVodFileColumn.SERIES_NUMBER.key(), "");
        rows.put(BtVodFileColumn.PRODUCT_ID.key(), PRODUCT_ID);
        rows.put(BtVodFileColumn.EPISODE_TITLE.key(), EPISODE_TITLE);
        rows.put(BtVodFileColumn.SERVICE_FORMAT.key(), "Youview");
        rows.put(BtVodFileColumn.IS_SERIES.key(), "N");
        rows.put(BtVodFileColumn.CATEGORY.key(), "");
        rows.put(BtVodFileColumn.AVAILABILITY_START.key(), "Apr  1 2013 12:00AM");
        rows.put(BtVodFileColumn.AVAILABILITY_END.key(), "Apr 30 2014 12:00AM");
        rows.put(BtVodFileColumn.PACKSHOT.key(), IMAGE_FILENAME);
        rows.put(BtVodFileColumn.SYNOPSIS.key(), SYNOPSIS);
        rows.put(BtVodFileColumn.PRODUCT_YOUVIEW_DURATION.key(), "PT0H03M39.458S");
        rows.put(BtVodFileColumn.METADATA_DURATION.key(), "PT00H03M36S");
        
        Map<String, String> map = rows.build();
        return new BtVodDataRow(ImmutableList.copyOf(map.values()), 
                ImmutableList.copyOf(map.keySet()));
    }
    
    private String itemUri() {
        return URI_PREFIX + "items/" + PRODUCT_ID;
    }
}
