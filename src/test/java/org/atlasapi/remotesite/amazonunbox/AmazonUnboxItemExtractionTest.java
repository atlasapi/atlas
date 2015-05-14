package org.atlasapi.remotesite.amazonunbox;

import static org.atlasapi.remotesite.amazonunbox.AmazonUnboxGenre.ACTION;
import static org.atlasapi.remotesite.amazonunbox.AmazonUnboxGenre.ADVENTURE;
import static org.atlasapi.remotesite.amazonunbox.AmazonUnboxGenre.THRILLER;
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


public class AmazonUnboxItemExtractionTest {
    
    @Test
    public void testParsingSingleItemUsingSax() throws ParserConfigurationException, SAXException, IOException {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser saxParser = factory.newSAXParser();
        
        TestAmazonUnboxProcessor processor = new TestAmazonUnboxProcessor();
        AmazonUnboxContentHandler handler = new AmazonUnboxContentHandler(processor);
        saxParser.parse(getFileAsInputStream("single_item.xml"), handler);
        
        AmazonUnboxItem item = Iterables.getOnlyElement(processor.getItems());
        
        assertEquals(2.0f, item.getAmazonRating(), 0.0001f);
        assertThat(item.getAmazonRatingsCount(), is(equalTo(7)));
        assertEquals("B007FUIBHM", item.getAsin());
        assertEquals(ContentType.MOVIE, item.getContentType());
        assertEquals("Liz Adams", item.getDirector());
        assertEquals(ImmutableSet.of(ACTION, ADVENTURE, THRILLER), item.getGenres());
        assertEquals("http://ecx.images-amazon.com/images/I/51LG6PC6P1L._SX320_SY240_.jpg", item.getLargeImageUrl());
        assertEquals(Quality.SD, item.getQuality());
        assertEquals(Boolean.FALSE, item.isPreOrder());
        assertEquals(Boolean.FALSE, item.isRental());
        assertEquals(Boolean.FALSE, item.isSeasonPass());
        assertEquals(Boolean.TRUE, item.isStreamable());
        assertEquals(
                "When a solar storm wipes out the air traffic control system, Air Force One and a passenger jet liner "
                + "are locked on a collision course in the skies.",
                item.getSynopsis()
        );
        assertEquals("NR", item.getRating());
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
    
    private class TestAmazonUnboxProcessor implements AmazonUnboxProcessor<UpdateProgress> {

        private UpdateProgress progress = UpdateProgress.START;
        private final List<AmazonUnboxItem> items = Lists.newArrayList();
        
        @Override
        public boolean process(AmazonUnboxItem aUItem) {
            items.add(aUItem);
            progress = progress.reduce(UpdateProgress.SUCCESS);
            return true;
        }

        @Override
        public UpdateProgress getResult() {
            return progress;
        }
        
        public List<AmazonUnboxItem> getItems() {
            return items;
        }
    }
}
