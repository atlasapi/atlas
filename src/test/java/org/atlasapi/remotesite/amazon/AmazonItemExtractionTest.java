package org.atlasapi.remotesite.amazon;

import static org.atlasapi.remotesite.amazon.AmazonGenre.ACTION;
import static org.atlasapi.remotesite.amazon.AmazonGenre.ADVENTURE;
import static org.atlasapi.remotesite.amazon.AmazonGenre.THRILLER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.metabroadcast.common.scheduling.UpdateProgress;


public class AmazonItemExtractionTest {
    
    @Test
    public void testParsingSingleItemUsingSax() throws ParserConfigurationException, SAXException, IOException {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser saxParser = factory.newSAXParser();
        
        TestAmazonProcessor processor = new TestAmazonProcessor();
        AmazonContentHandler handler = new AmazonContentHandler(processor);
        saxParser.parse(getFileAsInputStream("single_item.xml"), handler);
        
        AmazonItem item = Iterables.getOnlyElement(processor.getItems());
        
        assertEquals(2.0f, item.getAmazonRating(), 0.0001f);
        assertThat(item.getAmazonRatingsCount(), is(equalTo(7)));
        assertEquals("B007FUIBHM", item.getAsin());
        assertEquals(ContentType.MOVIE, item.getContentType());
        assertEquals(ImmutableSet.of("Liz Adams","Superman"), item.getDirectors());
        assertEquals(ImmutableSet.of(ACTION, ADVENTURE, THRILLER), item.getGenres());
        assertEquals("http://ecx.images-amazon.com/images/I/51LG6PC6P1L._SX320_SY240_.jpg", item.getLargeImageUrl());
        assertEquals(Quality.SD, item.getQuality());
        assertEquals(Boolean.FALSE, item.isPreOrder());
        assertEquals(Boolean.FALSE, item.isRental());
        assertEquals(Boolean.FALSE, item.isSeasonPass());
        assertEquals(Boolean.TRUE, item.isStreamable());
        assertEquals(null, item.getUnboxHdPurchaseUrl());
        assertEquals("http://www.amazon.com/gp/product/B0091VSK3S/INSERT_TAG_HERE/ref=atv_feed_catalog/", item.getUnboxSdPurchaseUrl());
        assertEquals(
                "When a solar storm wipes out the air traffic control system, Air Force One and a passenger jet liner "
                + "are locked on a collision course in the skies.",
                item.getSynopsis()
        );
        assertEquals("bbfc_rating|ages_12_and_over", item.getRating());
        assertEquals("9.99", item.getPrice());
        assertEquals(new DateTime(2011, 12, 31, 0, 0, 0).withZone(DateTimeZone.forID("Europe/London")), item.getReleaseDate());
        assertEquals(Duration.standardMinutes(93), item.getDuration());
        assertEquals(ImmutableSet.of("Reginald VelJohnson", "Jordan Ladd"), item.getStarring());
        assertEquals("Millennium Entertainment", item.getStudio());
        assertEquals("tt2091229", item.getTConst());
        assertEquals("Air Collision", item.getTitle());
        assertEquals(Boolean.TRUE, item.isTivoEnabled());
        assertEquals("http://www.amazon.com/gp/product/B007FUIBHM/ref=atv_feed_catalog", item.getUrl());
    }
    
    private InputStream getFileAsInputStream(String fileName) throws IOException {
        URL testFile = Resources.getResource(getClass(), fileName);
        return Resources.newInputStreamSupplier(testFile).getInput();
    }
    
    private class TestAmazonProcessor implements AmazonProcessor<UpdateProgress> {

        private UpdateProgress progress = UpdateProgress.START;
        private final List<AmazonItem> items = Lists.newArrayList();
        
        @Override
        public boolean process(AmazonItem aUItem) {
            items.add(aUItem);
            progress = progress.reduce(UpdateProgress.SUCCESS);
            return true;
        }

        @Override
        public UpdateProgress getResult() {
            return progress;
        }
        
        public List<AmazonItem> getItems() {
            return items;
        }
    }
}
