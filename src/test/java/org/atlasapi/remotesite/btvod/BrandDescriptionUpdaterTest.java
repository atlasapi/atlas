package org.atlasapi.remotesite.btvod;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Series;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BrandDescriptionUpdaterTest {

    private BrandDescriptionUpdater updater;

    @Before
    public void setUp() throws Exception {
        updater = new BrandDescriptionUpdater();
    }

    @Test
    public void testDoNotUpdateIfBrandHasOwnDescription() throws Exception {
        String brandDescription = "brandDescription";
        String brandLongDescription = "brandLongDescription";
        Brand brand = getBrand("uri", brandDescription, brandLongDescription);

        updater.updateDescriptions(brand,
                getSeries("seriesDescription", "seriesLongDescription", 5));

        assertThat(brand.getDescription(), is(brandDescription));
        assertThat(brand.getLongDescription(), is(brandLongDescription));
    }

    @Test
    public void testUpdateIfBrandHasNoDescription() throws Exception {
        Brand brand = getBrand("uri", null, null);

        Series series = getSeries("1", "1L", 1);

        updater.updateDescriptions(brand, series);
        assertThat(brand.getDescription(), is(series.getDescription()));
        assertThat(brand.getLongDescription(), is(series.getLongDescription()));
    }

    @Test
    public void testDoNotUpdateIfBrandHasTheDescriptionFromAnOlderSeries() throws Exception {
        Brand brand = getBrand("uri", null, null);

        Series olderSeries = getSeries("5", "5L", 5);
        Series newerSeries = getSeries("6", "6L",6);

        updater.updateDescriptions(brand, olderSeries);
        assertThat(brand.getDescription(), is(olderSeries.getDescription()));
        assertThat(brand.getLongDescription(), is(olderSeries.getLongDescription()));

        updater.updateDescriptions(brand, newerSeries);
        assertThat(brand.getDescription(), is(olderSeries.getDescription()));
        assertThat(brand.getLongDescription(), is(olderSeries.getLongDescription()));
    }

    @Test
    public void testUpdateIfBrandHasTheDescriptionFromNewerSeries() throws Exception {
        Brand brand = getBrand("uri", null, null);

        Series olderSeries = getSeries("5", "5L", 5);
        Series newerSeries = getSeries("6", "6L", 6);

        updater.updateDescriptions(brand, newerSeries);
        assertThat(brand.getDescription(), is(newerSeries.getDescription()));
        assertThat(brand.getLongDescription(), is(newerSeries.getLongDescription()));

        updater.updateDescriptions(brand, olderSeries);
        assertThat(brand.getDescription(), is(olderSeries.getDescription()));
        assertThat(brand.getLongDescription(), is(olderSeries.getLongDescription()));
    }

    @Test
    public void testGetDescriptionFromOldestSeriesWithNonNullDescription() throws Exception {
        Brand brand = getBrand("uri", null, null);

        Series olderSeries = getSeries(null, null, 5);
        Series newerSeries = getSeries("6", "6L", 6);

        updater.updateDescriptions(brand, newerSeries);
        assertThat(brand.getDescription(), is(newerSeries.getDescription()));
        assertThat(brand.getLongDescription(), is(newerSeries.getLongDescription()));

        updater.updateDescriptions(brand, olderSeries);
        assertThat(brand.getDescription(), is(newerSeries.getDescription()));
        assertThat(brand.getLongDescription(), is(newerSeries.getLongDescription()));
    }

    private Brand getBrand(String uri, String description, String longDescription) {
        Brand brand = new Brand();
        brand.setCanonicalUri(uri);
        brand.setDescription(description);
        brand.setLongDescription(longDescription);

        return brand;
    }

    private Series getSeries(String description, String longDescription, int seriesNumber) {
        Series series = new Series();
        series.setDescription(description);
        series.setLongDescription(longDescription);
        series.withSeriesNumber(seriesNumber);

        return series;
    }
}