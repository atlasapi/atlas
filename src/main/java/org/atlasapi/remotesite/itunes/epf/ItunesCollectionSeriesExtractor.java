package org.atlasapi.remotesite.itunes.epf;

import static org.atlasapi.remotesite.itunes.epf.model.EpfCollection.COLLECTION_ID;
import static org.atlasapi.remotesite.itunes.epf.model.EpfCollection.NAME;

import com.google.common.annotations.VisibleForTesting;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.itunes.epf.model.EpfCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;


public class ItunesCollectionSeriesExtractor implements ContentExtractor<EpfCollection, Series> {

    private static final Logger log = LoggerFactory.getLogger(ItunesCollectionSeriesExtractor.class);

    // Different language series. Allows series + extra character to account for "series:" or "series," but not
    // plurals like "seasons 4, 5, 6"
    private final String SERIES_REGEX = "(?i)series[\\W]?";
    private final String SEASON_REGEX = "(?i)season[\\W]?";
    private final String SAISON_REGEX = "(?i)saison[\\W]?";
    private final String STAFFEL_REGEX = "(?i)staffel[\\W]?";

    // Allows a number to be preceded by one character ("#42")
    private final String NUMBER = ".?\\d+[,|:]?";

    private final String SINGLE_DASH_OR_COLON = "[-|:]?";

    @Override
    public Series extract(EpfCollection collection) {
        Integer collectionId = collection.get(COLLECTION_ID);
        
        Series series = new Series(
                "http://itunes.apple.com/tv-season/id"+collectionId,
                "itunes:t-"+collectionId,
                Publisher.ITUNES
        );
        series.setTitle(collection.get(NAME));
        series.withSeriesNumber(tryExtractSeriesNumber(collection.get(NAME)));

        return series;
    }

    @VisibleForTesting
    @Nullable
    Integer tryExtractSeriesNumber(String name) {

        String[] nameParts = name.split(" ");

        for (int i = 0; i < nameParts.length - 1; i++) {

            // If we don't find an allowed variation of "series", keep looking
            if (!foundIdentifier(nameParts[i])) {
                continue;
            }

            // Matches on ".3" or "#34" etc.
            if (nameParts[i+1].matches(NUMBER)) {
                return parseInt(nameParts[i+1]);

            // Allows characters separating series and number "series - 4"
            } else if (nameParts[i+1].matches(SINGLE_DASH_OR_COLON) &&
                    nameParts.length > i+2 &&
                    nameParts[i+2].matches(NUMBER)) {

                return parseInt(nameParts[i+2]);
            }
        }

        log.info("No series number extracted from: {}", name);

        return null;
    }

    private boolean foundIdentifier(String part) {
        return (part.matches(SERIES_REGEX) ||
                part.matches(SEASON_REGEX) ||
                part.matches(SAISON_REGEX) ||
                part.matches(STAFFEL_REGEX)
        );
    }

    @Nullable
    private Integer parseInt(String num) {
        String parsedString = num.replaceAll("[^0-9]", "");

        // numbers of length 4 or more are either years or something else so ignore them
        if(parsedString.length() < 4) {
            return Integer.parseInt(parsedString);
        } else {
            return null;
        }
    }

}
