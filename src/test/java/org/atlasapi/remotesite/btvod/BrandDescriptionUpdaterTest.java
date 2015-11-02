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
        Brand brand = getBrand("uri", brandDescription);

        updater.updateDescription(brand, getSeries("seriesDescription", 5));

        assertThat(brand.getDescription(), is(brandDescription));
    }

    @Test
    public void testUpdateIfBrandHasNoDescription() throws Exception {
        Brand brand = getBrand("uri", null);

        Series series = getSeries("1", 1);

        updater.updateDescription(brand, series);
        assertThat(brand.getDescription(), is(series.getDescription()));
    }

    @Test
    public void testDoNotUpdateIfBrandHasTheDescriptionFromAnOlderSeries() throws Exception {
        Brand brand = getBrand("uri", null);

        Series olderSeries = getSeries("5", 5);
        Series newerSeries = getSeries("6", 6);

        updater.updateDescription(brand, olderSeries);
        assertThat(brand.getDescription(), is(olderSeries.getDescription()));

        updater.updateDescription(brand, newerSeries);
        assertThat(brand.getDescription(), is(olderSeries.getDescription()));
    }

    @Test
    public void testUpdateIfBrandHasTheDescriptionFromNewerSeries() throws Exception {
        Brand brand = getBrand("uri", null);

        Series olderSeries = getSeries("5", 5);
        Series newerSeries = getSeries("6", 6);

        updater.updateDescription(brand, newerSeries);
        assertThat(brand.getDescription(), is(newerSeries.getDescription()));

        updater.updateDescription(brand, olderSeries);
        assertThat(brand.getDescription(), is(olderSeries.getDescription()));
    }

    private Brand getBrand(String uri, String description) {
        Brand brand = new Brand();
        brand.setCanonicalUri(uri);
        brand.setDescription(description);

        return brand;
    }

    private Series getSeries(String description, int seriesNumber) {
        Series series = new Series();
        series.setDescription(description);
        series.withSeriesNumber(seriesNumber);

        return series;
    }
}