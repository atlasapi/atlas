package org.atlasapi.remotesite.amazonunbox;

import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.base.Splitter;



public class AmazonUnboxContentHandler extends DefaultHandler {
    
    private static final Splitter SPLIT_ON_COMMA = Splitter.on(',').trimResults().omitEmptyStrings();
    
    private final Logger log = LoggerFactory.getLogger(AmazonUnboxContentHandler.class);
    private final DateTimeFormatter dateParser = ISODateTimeFormat.dateTimeParser().withZone(DateTimeZone.forID("Europe/London"));
    private final AmazonUnboxProcessor<?> processor;
    
    private AmazonUnboxItem.Builder item = null;
    
    private ItemField currentField = null;
    private StringBuffer buffer = null; 

    public AmazonUnboxContentHandler(AmazonUnboxProcessor<?> processor) {
        this.processor = processor;
    }
    
    @Override
    public void startElement (String uri, String localName, String qName, Attributes attributes) throws SAXException {
        if (item != null) {
            currentField = ItemField.valueOf(qName);
            buffer = new StringBuffer();
        } else if (qName.equalsIgnoreCase("Item")) {
            item = AmazonUnboxItem.builder();
        }
    }

    @Override
    public void endElement (String uri, String localName, String qName) throws SAXException {
        if (qName.equalsIgnoreCase("Item")) {
            processor.process(item.build());
            item = null;
        } 
        if (currentField != null) {
            // TODO remove unused cases
            switch (ItemField.valueOf(qName)) {
            case AMAZONRATINGS:
                item.withAmazonRating(Float.valueOf(buffer.toString()));
                break;
            case AMAZONRATINGSCOUNT:
                item.withAmazonRatingsCount(Integer.valueOf(buffer.toString()));
                break;
            case ASIN:
                item.withAsin(buffer.toString());
                break;
            case CONTENTTYPE:
                item.withContentType(ContentType.valueOf(buffer.toString().toUpperCase()));
                break;
            case DIRECTOR:
                item.withDirector(buffer.toString());
                break;
            case EPISODENUMBER:
                item.withEpisodeNumber(Integer.valueOf(buffer.toString()));
                break;
            case GENRE_ACTION:
                item.withGenre(AmazonUnboxGenre.ACTION);
                break;
            case GENRE_ADVENTURE:
                item.withGenre(AmazonUnboxGenre.ADVENTURE);
                break;
            case GENRE_ANIMATION:
                item.withGenre(AmazonUnboxGenre.ANIMATION);
                break;
            case GENRE_BIOGRAPHY:
                item.withGenre(AmazonUnboxGenre.BIOGRAPHY);
                break;
            case GENRE_COMEDY:
                item.withGenre(AmazonUnboxGenre.COMEDY);
                break;
            case GENRE_CRIME:
                item.withGenre(AmazonUnboxGenre.CRIME);
                break;
            case GENRE_DOCUMENTARY:
                item.withGenre(AmazonUnboxGenre.DOCUMENTARY);
                break;
            case GENRE_DRAMA:
                item.withGenre(AmazonUnboxGenre.DRAMA);
                break;
            case GENRE_FAMILY:
                item.withGenre(AmazonUnboxGenre.FAMILY);
                break;
            case GENRE_FANTASY:
                item.withGenre(AmazonUnboxGenre.FANTASY);
                break;
            case GENRE_FILMNOIR:
                item.withGenre(AmazonUnboxGenre.FILMNOIR);
                break;
            case GENRE_GAMESHOW:
                item.withGenre(AmazonUnboxGenre.GAMESHOW);
                break;
            case GENRE_GAYLESBIAN:
                item.withGenre(AmazonUnboxGenre.GAYLESBIAN);
                break;
            case GENRE_HISTORY:
                item.withGenre(AmazonUnboxGenre.HISTORY);
                break;
            case GENRE_HORROR:
                item.withGenre(AmazonUnboxGenre.HORROR);
                break;
            case GENRE_INDEPENDENTFILM:
                item.withGenre(AmazonUnboxGenre.INDEPENDENTFILM);
                break;
            case GENRE_INTERNATIONAL:
                item.withGenre(AmazonUnboxGenre.INTERNATIONAL);
                break;
            case GENRE_MUSIC:
                item.withGenre(AmazonUnboxGenre.MUSIC);
                break;
            case GENRE_MUSICAL:
                item.withGenre(AmazonUnboxGenre.MUSICAL);
                break;
            case GENRE_MYSTERY:
                item.withGenre(AmazonUnboxGenre.MYSTERY);
                break;
            case GENRE_NONFICTION:
                item.withGenre(AmazonUnboxGenre.NONFICTION);
                break;
            case GENRE_REALITYTV:
                item.withGenre(AmazonUnboxGenre.REALITYTV);
                break;
            case GENRE_ROMANCE:
                item.withGenre(AmazonUnboxGenre.ROMANCE);
                break;
            case GENRE_SCIFI:
                item.withGenre(AmazonUnboxGenre.SCIFI);
                break;
            case GENRE_SHORT:
                item.withGenre(AmazonUnboxGenre.SHORT);
                break;
            case GENRE_SPORT:
                item.withGenre(AmazonUnboxGenre.SPORT);
                break;
            case GENRE_TALKSHOW:
                item.withGenre(AmazonUnboxGenre.TALKSHOW);
                break;
            case GENRE_THRILLER:
                item.withGenre(AmazonUnboxGenre.THRILLER);
                break;
            case GENRE_WAR:
                item.withGenre(AmazonUnboxGenre.WAR);
                break;
            case GENRE_WESTERN:
                item.withGenre(AmazonUnboxGenre.WESTERN);
                break;
            case HASIMAGE:
                break;
            case IMAGE_URL_LARGE:
                item.withLargeImageUrl(buffer.toString());
                break;
            case IMAGE_URL_SMALL:
                break;
            case ISPREORDER:
                item.withPreOrder(parseBoolean(buffer.toString()));
                break;
            case ISRENTAL:
                item.withRental(parseBoolean(buffer.toString()));
                break;
            case ISSEASONPASS:
                item.withSeasonPass(parseBoolean(buffer.toString()));
                break;
            case ISSTREAMABLE:
                item.withStreamable(parseBoolean(buffer.toString()));
                break;
            case ISTRIDENT:
                break;
            case LONGSYNOPSIS:
                break;
            case MPAARATING:
                item.withRating(buffer.toString());
                break;
            case PLOTOUTLINE:
                break;
            case PRICE:
                item.withPrice(buffer.toString());
                break;
            case QUALITY:
                item.withQuality(Quality.valueOf(buffer.toString().toUpperCase()));
                break;
            case RELATED_PRODUCTS:
                break;
            case RELEASEDATE:
                item.withReleaseDate(dateParser.parseDateTime(buffer.toString()));
                break;
            case RUNTIME:
                item.withDuration(Duration.standardMinutes(Long.valueOf(buffer.toString())));
                break;
            case SEASONASIN:
                item.withSeasonAsin(buffer.toString());
                break;
            case SEASONNUMBER:
                item.withSeasonNumber(Integer.valueOf(buffer.toString()));
                break;
            case SERIESASIN:
                item.withSeriesAsin(buffer.toString());
                break;
            case SERIESTITLE:
                item.withSeriesTitle(buffer.toString());
                break;
            case STARRING:
                item.withStarringRoles(SPLIT_ON_COMMA.split(buffer.toString()));
                break;
            case STUDIO:
                item.withStudio(buffer.toString());
                break;
            case SYNOPSIS:
                item.withSynopsis(buffer.toString());
                break;
            case TCONST:
                item.withTConst(buffer.toString());
                break;
            case TITLE:
                item.withTitle(buffer.toString());
                break;
            case TIVOENABLED:
                item.withTivoEnabled(parseBoolean(buffer.toString()));
                break;
            case TRAILER_STREAM_URL_1:
                break;
            case TRAILER_STREAM_URL_2:
                break;
            case TRAILER_STREAM_URL_3:
                break;
            case TRAILER_STREAM_URL_4:
                break;
            case TRAILER_STREAM_URL_5:
                break;
            case UNBOX_HD_PURCHASE_ASIN:
                break;
            case UNBOX_HD_PURCHASE_PRICE:
                break;
            case UNBOX_HD_PURCHASE_URL:
                break;
            case UNBOX_HD_RENTAL_ASIN:
                break;
            case UNBOX_HD_RENTAL_PRICE:
                break;
            case UNBOX_HD_RENTAL_URL:
                break;
            case UNBOX_PURCHASE_ASIN:
                break;
            case UNBOX_PURCHASE_PRICE:
                break;
            case UNBOX_PURCHASE_URL:
                break;
            case UNBOX_RENTAL_ASIN:
                break;
            case UNBOX_RENTAL_PRICE:
                break;
            case UNBOX_RENTAL_URL:
                break;
            case UNBOX_SD_PURCHASE_ASIN:
                break;
            case UNBOX_SD_PURCHASE_PRICE:
                break;
            case UNBOX_SD_PURCHASE_URL:
                break;
            case UNBOX_SD_RENTAL_ASIN:
                break;
            case UNBOX_SD_RENTAL_PRICE:
                break;
            case UNBOX_SD_RENTAL_URL:
                break;
            case URL:
                item.withUrl(buffer.toString());
                break;
            default:
                //TODO change this log level
                log.info("Field " + qName + " not currently processed");
                break;
            }
            buffer = null;
            currentField = null;
        }
    }
    
    @Override
    public void characters(char ch[], int start, int length) throws SAXException {
        if (buffer != null) {
            for (int i=start; i<start+length; i++) {
                buffer.append(ch[i]);
            }
//        } else {
//            log.error("String buffer not initialised");
        }
    }
    
    private Boolean parseBoolean(String input) {
        if (input.equals("Y")) {
            return true;
        } else if (input.equals("N")) {
            return false;
        }
        return null;
    }
}
