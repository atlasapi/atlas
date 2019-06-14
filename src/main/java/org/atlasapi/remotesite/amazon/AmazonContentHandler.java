package org.atlasapi.remotesite.amazon;

import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import static com.google.common.base.Preconditions.checkNotNull;

public class AmazonContentHandler extends DefaultHandler {
    
    private static final Splitter SPLIT_ON_COMMA =
            Splitter.on(',').trimResults().omitEmptyStrings();
    //names that should not be allowed through as actor/director names, in lowercase.
    private static final ImmutableSet<String> KNOWN_NON_REAL_NAMES = ImmutableSet.of("unavailable");
    private static final Pattern TWO_ALPHA_CHARS = Pattern.compile(".*[a-zA-Z].*[a-zA-Z].*");
    
    private final Logger log = LoggerFactory.getLogger(AmazonContentHandler.class);
    private final DateTimeFormatter dateParser =
            ISODateTimeFormat.dateTimeParser().withZone(DateTimeZone.forID("Europe/London"));
    private final AmazonProcessor<?> processor;
    
    private AmazonItem.Builder item = null;
    
    private ItemField currentField = null;
    private StringBuffer buffer = null;
    //anything nested in the following blocks will be ignored. Will only work on single lvl of nesting.
    private static final Set<ItemField> IGNORED_BLOCKS = ImmutableSet.of(
            ItemField.RELATED_PRODUCTS);
    private boolean ignoreBlock = false;

    public AmazonContentHandler(AmazonProcessor<?> processor) {
        this.processor = checkNotNull(processor);
    }
    
    @Override
    public void startElement (
            String uri,
            String localName, String qName, Attributes attributes) throws SAXException {

        if (item != null) {
            currentField = ItemField.valueOf(qName);
            //ignore everything inside this field.
            if (IGNORED_BLOCKS.contains(currentField)) {
                ignoreBlock = true; //Ending the field above will set this to false;
            }
            buffer = new StringBuffer();
        } else if (qName.equalsIgnoreCase("Item")) {
            item = AmazonItem.builder();
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (qName.equalsIgnoreCase("Item")) {
            processor.process(item.build());
            ignoreBlock = false; //safety precaution
            item = null;
            return;
        }
        if (currentField != null) {
            ItemField itemField = ItemField.valueOf(qName);
            if(IGNORED_BLOCKS.contains(itemField)){
                ignoreBlock = false; //stop ignoring stuff
                return;
            }
            if (ignoreBlock) {
                return; //if the current field was inside a block we are ignoring, do nothing.
            }

            // TODO remove unused cases
            switch (itemField) {
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
                item.addDirectorRoles(splitAndClean(buffer.toString()));
                break;
            case EPISODENUMBER:
                item.withEpisodeNumber(Integer.valueOf(buffer.toString()));
                break;
            case GENRE_ACTION:
                item.withGenre(AmazonGenre.ACTION);
                break;
            case GENRE_ADVENTURE:
                item.withGenre(AmazonGenre.ADVENTURE);
                break;
            case GENRE_ANIMATION:
                item.withGenre(AmazonGenre.ANIMATION);
                break;
            case GENRE_BIOGRAPHY:
                item.withGenre(AmazonGenre.BIOGRAPHY);
                break;
            case GENRE_COMEDY:
                item.withGenre(AmazonGenre.COMEDY);
                break;
            case GENRE_CRIME:
                item.withGenre(AmazonGenre.CRIME);
                break;
            case GENRE_DOCUMENTARY:
                item.withGenre(AmazonGenre.DOCUMENTARY);
                break;
            case GENRE_DRAMA:
                item.withGenre(AmazonGenre.DRAMA);
                break;
            case GENRE_FAMILY:
                item.withGenre(AmazonGenre.FAMILY);
                break;
            case GENRE_FANTASY:
                item.withGenre(AmazonGenre.FANTASY);
                break;
            case GENRE_FILMNOIR:
                item.withGenre(AmazonGenre.FILMNOIR);
                break;
            case GENRE_GAMESHOW:
                item.withGenre(AmazonGenre.GAMESHOW);
                break;
            case GENRE_GAYLESBIAN:
                item.withGenre(AmazonGenre.GAYLESBIAN);
                break;
            case GENRE_HISTORY:
                item.withGenre(AmazonGenre.HISTORY);
                break;
            case GENRE_HORROR:
                item.withGenre(AmazonGenre.HORROR);
                break;
            case GENRE_INDEPENDENTFILM:
                item.withGenre(AmazonGenre.INDEPENDENTFILM);
                break;
            case GENRE_INTERNATIONAL:
                item.withGenre(AmazonGenre.INTERNATIONAL);
                break;
            case GENRE_MUSIC:
                item.withGenre(AmazonGenre.MUSIC);
                break;
            case GENRE_MUSICAL:
                item.withGenre(AmazonGenre.MUSICAL);
                break;
            case GENRE_MYSTERY:
                item.withGenre(AmazonGenre.MYSTERY);
                break;
            case GENRE_NONFICTION:
                item.withGenre(AmazonGenre.NONFICTION);
                break;
            case GENRE_REALITYTV:
                item.withGenre(AmazonGenre.REALITYTV);
                break;
            case GENRE_ROMANCE:
                item.withGenre(AmazonGenre.ROMANCE);
                break;
            case GENRE_SCIFI:
                item.withGenre(AmazonGenre.SCIFI);
                break;
            case GENRE_SHORT:
                item.withGenre(AmazonGenre.SHORT);
                break;
            case GENRE_SPORT:
                item.withGenre(AmazonGenre.SPORT);
                break;
            case GENRE_TALKSHOW:
                item.withGenre(AmazonGenre.TALKSHOW);
                break;
            case GENRE_THRILLER:
                item.withGenre(AmazonGenre.THRILLER);
                break;
            case GENRE_WAR:
                item.withGenre(AmazonGenre.WAR);
                break;
            case GENRE_WESTERN:
                item.withGenre(AmazonGenre.WESTERN);
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
                item.withIsTrident(parseBoolean(buffer.toString()));
                break;
            case LONGSYNOPSIS:
                break;
            case CANONICAL_MATURITY_RATING:
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
                item.addStarringRoles(splitAndClean(buffer.toString()));
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
            case UNBOX_HD_RENTAL_ASIN:
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
                item.withUnboxSdPurchasePrice(buffer.toString());
                break;
            case UNBOX_SD_PURCHASE_URL:
                item.withUnboxSdPurchaseUrl(buffer.toString());
                break;
            case UNBOX_SD_RENTAL_PRICE:
                item.withUnboxSdRentalPrice(buffer.toString());
                break;
            case UNBOX_SD_RENTAL_URL:
                item.withUnboxSdRentalUrl(buffer.toString());
                break;
            case UNBOX_HD_RENTAL_PRICE:
                item.withUnboxHdRentalPrice(buffer.toString());
                break;
            case UNBOX_HD_RENTAL_URL:
                item.withUnboxHdRentalUrl(buffer.toString());
                break;
            case UNBOX_HD_PURCHASE_PRICE:
                item.withUnboxHdPurchasePrice(buffer.toString());
                break;
            case UNBOX_HD_PURCHASE_URL:
                item.withUnboxHdPurchaseUrl(buffer.toString());
                break;
            case URL:
                item.withUrl(buffer.toString());
                break;
            default:
                log.debug("Field " + qName + " not currently processed");
                break;
            }
            buffer = null;
            currentField = null;
        }
    }

    private Iterable<String> splitAndClean(String stringList) {

        return StreamSupport.stream(SPLIT_ON_COMMA.split(stringList)
                .spliterator(), false)
                .filter(i -> TWO_ALPHA_CHARS.matcher(i).matches())
                .filter(i -> !KNOWN_NON_REAL_NAMES.contains(i.toLowerCase()))
                .collect(MoreCollectors.toImmutableSet());
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
