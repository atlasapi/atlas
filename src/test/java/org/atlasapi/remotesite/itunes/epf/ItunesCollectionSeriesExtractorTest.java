package org.atlasapi.remotesite.itunes.epf;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ItunesCollectionSeriesExtractorTest {

    private ItunesCollectionSeriesExtractor extractor = new ItunesCollectionSeriesExtractor();

    @Test
    public void seriesNumberExtracterGetsSeriesNumber() {

        String standardCase = "Humans, Series 1";
        String seriesNumberWithHash = "Split saison #3 - Single";
        String seriesSeperatedByColon = "The Cool staffel: 4";
        String allCaps = "THE COOL SERIES 10";
        String mixedCase = "ThE cOoL sEaSoN 1";

        assertThat(extractor.tryExtractSeriesNumber(standardCase), is(1));
        assertThat(extractor.tryExtractSeriesNumber(seriesNumberWithHash), is(3));
        assertThat(extractor.tryExtractSeriesNumber(seriesSeperatedByColon), is(4));
        assertThat(extractor.tryExtractSeriesNumber(allCaps), is(10));
        assertThat(extractor.tryExtractSeriesNumber(mixedCase), is(1));

    }

    @Test
    public void seriesNumberExtractorIgnoresSeriesNumber() {

        String seriesWithNoNumber = "TV Screen .1 (Orchestral pieces for shows, series & documentaries";
        String seriesWithVolumeNumber = "Producer Series, Vol. 3 (Africa)";
        String seriesWithNumberFarAway = "Piano Series: Beethoven (Sonatas 4)";
        String seriesWithNumberConnectedToVol = "Gerry Mulligan Quartet, Zurich 1962 / Swiss Radio Days, Jazz Series Vol.9";
        String nameThatEndsWithSeries = "String with the word at the end. series";
        String nameWithYearFollowingSeries = "Complete Jazz Series 1944";
        String nameWithMultipleYearsFollowingSeries = "Complete Jazz Series 1945 - 1946";

        assertThat(extractor.tryExtractSeriesNumber(seriesWithNoNumber), is(0));
        assertThat(extractor.tryExtractSeriesNumber(seriesWithVolumeNumber), is(0));
        assertThat(extractor.tryExtractSeriesNumber(seriesWithNumberFarAway), is(0));
        assertThat(extractor.tryExtractSeriesNumber(seriesWithNumberConnectedToVol), is(0));
        assertThat(extractor.tryExtractSeriesNumber(nameThatEndsWithSeries), is(0));
        assertThat(extractor.tryExtractSeriesNumber(nameWithYearFollowingSeries), is(0));
        assertThat(extractor.tryExtractSeriesNumber(nameWithMultipleYearsFollowingSeries), is(0));

        // saison

    }
}
