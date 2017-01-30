package org.atlasapi.remotesite.itunes.epf;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.itunes.epf.model.EpfCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static org.atlasapi.remotesite.itunes.epf.model.EpfCollection.COLLECTION_ID;
import static org.atlasapi.remotesite.itunes.epf.model.EpfCollection.NAME;


public class ItunesCollectionSeriesExtractor implements ContentExtractor<EpfCollection, Series> {

    private static final Logger log = LoggerFactory.getLogger(ItunesCollectionSeriesExtractor.class);

    // Different language series. Allows series + extra character to account for "series:" or "series," but not
    // plurals like "seasons 4, 5, 6"
    private final String seriesRegex = "(?i)series[\\W]?";
    private final String seasonRegex = "(?i)season[\\W]?";
    private final String saisonRegex = "(?i)saison[\\W]?";
    private final String staffelRegex = "(?i)staffel[\\W]?";

    private final String singleDashOrColon = "[-|:]?";

    // Allows a number to be preceded by one character ("#42")
    private final String numberMatchingRegex = ".?\\d+[,|:]?";

    private final ImmutableList<String> numberWords = ImmutableList.of(
            "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve",
            "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen", "twenty"
    );

    private final ImmutableMap<String, Integer> wordToIntMap;

    private ItunesCollectionSeriesExtractor() {

        ImmutableMap.Builder<String, Integer> wordMapBuilder = ImmutableMap.builder();

        for(int i = 1; i <= numberWords.size(); i++) {
            wordMapBuilder.put(numberWords.get(i-1), i);
        }
        wordToIntMap = wordMapBuilder.build();
    }

    public static ItunesCollectionSeriesExtractor create() {
        return new ItunesCollectionSeriesExtractor();
    }

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
        series.setSpecialization(Specialization.TV);

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

            // Matches on ".3", "#34" or "3," etc.
            if (partRepresentsNumber(nameParts[i+1])) {
                return parseInt(nameParts[i+1]);
            }

            // Allows characters separating series and number "series - 4"
            if (nameParts[i+1].matches(singleDashOrColon) &&
                    nameParts.length > i+2 &&
                    partRepresentsNumber(nameParts[i+2])) {

                return parseInt(nameParts[i+2]);
            }
        }

        log.info("No series number extracted from: {}", name);
        return null;
    }

    private boolean foundIdentifier(String part) {
        return part.matches(seriesRegex) ||
                part.matches(seasonRegex) ||
                part.matches(saisonRegex) ||
                part.matches(staffelRegex);
    }

    private boolean partRepresentsNumber(String part) {
        return part.matches(numberMatchingRegex) || wordToIntMap.keySet().contains(removeNonAlphaChars(part));
    }

    @Nullable
    private Integer parseInt(String num) {

        if (wordToIntMap.keySet().contains(removeNonAlphaChars(num))) {
            return wordToIntMap.get(removeNonAlphaChars(num));
        }

        String parsedString = num.replaceAll("[^0-9]", "");

        // numbers of length 4 or more are either years or something else so ignore them
        if (parsedString.length() < 4) {
            return Integer.parseInt(parsedString);
        } else {
            return null;
        }
    }

    private String removeNonAlphaChars(String string) {
        return string.replaceAll("[^a-zA-Z]", "").toLowerCase();
    }

}
