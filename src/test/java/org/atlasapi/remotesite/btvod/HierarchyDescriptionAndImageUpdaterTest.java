package org.atlasapi.remotesite.btvod;

import java.util.Set;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class HierarchyDescriptionAndImageUpdaterTest {

    private HierarchyDescriptionAndImageUpdater updater;

    private String descriptionsGuidAliasNamespace;
    private String imagesGuidAliasNamespace;

    private Image firstImage;
    private Image secondImage;
    private Image thirdImage;

    @Before
    public void setUp() throws Exception {
        descriptionsGuidAliasNamespace = "descriptions namespace";
        imagesGuidAliasNamespace = "images namespace";

        updater = new HierarchyDescriptionAndImageUpdater(
                descriptionsGuidAliasNamespace, imagesGuidAliasNamespace
        );
        firstImage = new Image("firstImageUri");
        secondImage = new Image("secondImageUri");
        thirdImage = new Image("thirdImageUri");
    }

    @Test
    public void testDoNotUpdateIfTargetHasOwnDescription() throws Exception {
        String brandDescription = "brandDescription";
        String brandLongDescription = "brandLongDescription";
        Brand brand = getBrand("uri", brandDescription, brandLongDescription);

        updater.update(
                brand, getSeries("seriesDescription", "seriesLongDescription", 5), getRow("guid")
        );

        assertThat(brand.getDescription(), is(brandDescription));
        assertThat(brand.getLongDescription(), is(brandLongDescription));
    }

    @Test
    public void testDoNotUpdateIfTargetHasOwnImages() throws Exception {
        Brand brand = getBrand("uri", null, null, firstImage);

        updater.update(
                brand, getSeries("seriesDescription", "seriesLongDescription", 5), getRow("guid")
        );

        Set<Image> expectedImages = ImmutableSet.of(firstImage);
        assertThat(brand.getImages(), is(expectedImages));
        assertThat(brand.getImage(), is(firstImage.getCanonicalUri()));
    }

    @Test
    public void testUpdateIfTargetHasNoDescription() throws Exception {
        Brand brand = getBrand("uri", null, null);

        Series series = getSeries("1", "1L", 1);

        updater.update(brand, series, getRow("guid"));
        checkDescriptionsSource(brand, series);
    }

    @Test
    public void testUpdateIfTargetHasNoImages() throws Exception {
        Brand brand = getBrand("uri", null, null);

        Series series = getSeries("1", "1L", 1, firstImage);

        updater.update(brand, series, getRow("guid"));
        checkImagesSource(brand, series);
    }

    @Test
    public void testGetImagesAndDescriptionsFromOldestEpisodeWithNonNullData() throws Exception {
        Brand brand = getBrand("uri", null, null);

        Episode emptyEpisode = getEpisode(null, null, 4);
        Episode olderEpisode = getEpisode("5", "5L", 5, firstImage);
        Episode newerEpisode = getEpisode("6", "6L", 6, secondImage);

        updater.update(brand, newerEpisode, getRow("guid"));
        checkDescriptionsSource(brand, newerEpisode);
        checkImagesSource(brand, newerEpisode);

        updater.update(brand, olderEpisode, getRow("guid"));
        checkDescriptionsSource(brand, olderEpisode);
        checkImagesSource(brand, olderEpisode);

        updater.update(brand, emptyEpisode, getRow("guid"));
        checkDescriptionsSource(brand, olderEpisode);
        checkImagesSource(brand, olderEpisode);
    }

    @Test
    public void testGetImagesAndDescriptionsFromOldestSeriesWithNonNullData() throws Exception {
        Brand brand = getBrand("uri", null, null);

        Series emptySeries = getSeries(null, null, 4);
        Series olderSeries = getSeries("5", "5L", 5, firstImage);
        Series newerSeries = getSeries("6", "6L", 6, secondImage);

        updater.update(brand, newerSeries, getRow("guid"));
        checkDescriptionsSource(brand, newerSeries);
        checkImagesSource(brand, newerSeries);

        updater.update(brand, olderSeries, getRow("guid"));
        checkDescriptionsSource(brand, olderSeries);
        checkImagesSource(brand, olderSeries);

        updater.update(brand, emptySeries, getRow("guid"));
        checkDescriptionsSource(brand, olderSeries);
        checkImagesSource(brand, olderSeries);
    }

    @Test
    public void testGetImagesAndDescriptionsFromOldestCollectionWithNonNullData()
            throws Exception {
        Brand brand = getBrand("uri", null, null);

        BtVodCollection emptyCollection = new BtVodCollection(
                "guid", DateTime.now().minusDays(2), null, null, ImmutableSet.<Image>of()
        );
        BtVodCollection olderCollection = new BtVodCollection(
                "guid", DateTime.now().minusDays(1), "a", "aL", ImmutableSet.of(firstImage)
        );
        BtVodCollection newerCollection = new BtVodCollection(
                "guid", DateTime.now(), "b", "bL", ImmutableSet.of(secondImage)
        );

        updater.update(brand, newerCollection);
        checkDescriptionsSource(brand, newerCollection);
        checkImagesSource(brand, newerCollection);

        updater.update(brand, olderCollection);
        checkDescriptionsSource(brand, olderCollection);
        checkDescriptionsSource(brand, olderCollection);

        updater.update(brand, emptyCollection);
        checkDescriptionsSource(brand, olderCollection);
        checkImagesSource(brand, olderCollection);
    }

    @Test
    public void testUpdateFromSeriesIfBrandHasDescriptionsAndImagesFromEpisode() throws Exception {
        Brand brand = getBrand("uri", null, null);

        Episode episode = getEpisode("episode", "episodeL", 4, firstImage);
        Series series = getSeries("series", "seriesL", 5, secondImage);

        updater.update(brand, episode, getRow("guid"));
        checkDescriptionsSource(brand, episode);
        checkImagesSource(brand, episode);

        updater.update(brand, series, getRow("guid"));
        checkDescriptionsSource(brand, series);
        checkImagesSource(brand, series);
    }

    @Test
    public void testDoNotUpdateFromEpisodeIfBrandHasDescriptionsAndImagesFromSeries()
            throws Exception {
        Brand brand = getBrand("uri", null, null);

        Episode episode = getEpisode("episode", "episodeL", 4, firstImage);
        Series series = getSeries("series", "seriesL", 5, secondImage);

        updater.update(brand, series, getRow("guid"));
        checkDescriptionsSource(brand, series);
        checkImagesSource(brand, series);

        updater.update(brand, episode, getRow("guid"));
        checkDescriptionsSource(brand, series);
        checkImagesSource(brand, series);
    }

    @Test
    public void testUpdateFromCollectionIfBrandHasDescriptionsAndImagesFromSeries()
            throws Exception {
        Brand brand = getBrand("uri", null, null);

        Series series = getSeries("series", "seriesL", 5, firstImage);
        BtVodCollection collection = new BtVodCollection(
                "guid", DateTime.now(), "collection", "collectionL", ImmutableSet.of(secondImage)
        );

        updater.update(brand, series, getRow("guid"));
        checkDescriptionsSource(brand, series);
        checkImagesSource(brand, series);

        updater.update(brand, collection);
        checkDescriptionsSource(brand, collection);
        checkImagesSource(brand, collection);
    }

    @Test
    public void testDoNotUpdateFromSeriesOrEpisodeIfBrandHasDescriptionsAndImagesFromCollection()
            throws Exception {
        Brand brand = getBrand("uri", null, null);

        Episode episode = getEpisode("episode", "episodeL", 4, firstImage);
        Series series = getSeries("series", "seriesL", 5, secondImage);
        BtVodCollection collection = new BtVodCollection(
                "guid", DateTime.now(), "collection", "collectionL", ImmutableSet.of(thirdImage)
        );

        updater.update(brand, collection);
        checkDescriptionsSource(brand, collection);
        checkImagesSource(brand, collection);

        updater.update(brand, series, getRow("guid"));
        checkDescriptionsSource(brand, collection);
        checkImagesSource(brand, collection);

        updater.update(brand, episode, getRow("guid"));
        checkDescriptionsSource(brand, collection);
        checkImagesSource(brand, collection);
    }

    @Test
    public void testUpdateFromSeriesIfBrandHasDescriptionsAndImagesFromSeriesWithSameSeriesNumber()
            throws Exception {
        Brand brand = getBrand("uri", null, null);

        Series firstSeries = getSeries("firstSeries", "firstSeriesL", 5, firstImage);
        Series secondSeries = getSeries("secondSeries", "secondSeriesL", 5, secondImage);

        updater.update(brand, firstSeries, getRow("guid"));
        checkDescriptionsSource(brand, firstSeries);
        checkImagesSource(brand, firstSeries);

        updater.update(brand, secondSeries, getRow("guid"));
        checkDescriptionsSource(brand, secondSeries);
        checkImagesSource(brand, secondSeries);
    }

    @Test
    public void testDoNotUpdateAliasesWhenNotUpdatingImagesAndDescriptions() throws Exception {
        Brand brand = getBrand("uri", null, null);

        Alias unrelatedExistingAlias = new Alias("unrelated alias", "value");
        brand.setAliases(ImmutableList.of(unrelatedExistingAlias));

        Episode olderEpisode = getEpisode("5", "5L", 5, firstImage);
        BtVodEntry olderEpisodeRow = getRow("5-guid");

        Episode newerEpisode = getEpisode("6", "6L", 6, secondImage);
        BtVodEntry newerEpisodeRow = getRow("6-guid");

        updater.update(brand, olderEpisode, olderEpisodeRow);
        checkAliasSource(brand, olderEpisodeRow.getGuid(), unrelatedExistingAlias);

        updater.update(brand, newerEpisode, newerEpisodeRow);
        checkAliasSource(brand, olderEpisodeRow.getGuid(), unrelatedExistingAlias);
    }

    @Test
    public void testUpdateAliasesWhenUpdatingImages() throws Exception {
        Brand brand = getBrand("uri", "desc", "long-desc");

        Alias unrelatedExistingAlias = new Alias("unrelated alias", "value");
        brand.setAliases(ImmutableList.of(unrelatedExistingAlias));

        Episode episode = getEpisode(null, null, 6, secondImage);
        BtVodEntry episodeRow = getRow("6-guid");

        updater.update(brand, episode, episodeRow);
        checkAliasSource(brand, episodeRow.getGuid(), unrelatedExistingAlias);
    }

    @Test
    public void testUpdateAliasesWhenUpdatingDescriptions() throws Exception {
        Brand brand = getBrand("uri", null, null);

        Alias unrelatedExistingAlias = new Alias("unrelated alias", "value");
        brand.setAliases(ImmutableList.of(unrelatedExistingAlias));

        Episode episoe = getEpisode("5", "5L", 5);
        BtVodEntry episodeRow = getRow("5-guid");

        updater.update(brand, episoe, episodeRow);
        checkAliasSource(brand, episodeRow.getGuid(), unrelatedExistingAlias);
    }

    @Test
    public void testUpdateAliasesWhenUpdatingImagesAndDescriptionsFromCollection() throws Exception {
        Brand brand = getBrand("uri", null, null);

        Alias unrelatedExistingAlias = new Alias("unrelated alias", "value");
        brand.setAliases(ImmutableList.of(unrelatedExistingAlias));

        BtVodCollection collection = new BtVodCollection(
                "guid", DateTime.now(), "collection", "collectionL", ImmutableSet.of(firstImage)
        );

        updater.update(brand, collection);
        checkAliasSource(brand, collection.getGuid(), unrelatedExistingAlias);
    }

    private Brand getBrand(String uri, String description, String longDescription,
            Image... images) {
        Brand brand = new Brand();

        brand.setCanonicalUri(uri);
        brand.setDescription(description);
        brand.setLongDescription(longDescription);
        brand.setImages(ImmutableList.copyOf(images));
        if (images.length > 0) {
            brand.setImage(images[0].getCanonicalUri());
        }

        return brand;
    }

    private Series getSeries(String description, String longDescription, int seriesNumber,
            Image... images) {
        Series series = new Series();

        series.setDescription(description);
        series.setLongDescription(longDescription);
        series.withSeriesNumber(seriesNumber);
        series.setImages(ImmutableList.copyOf(images));
        if (images.length > 0) {
            series.setImage(images[0].getCanonicalUri());
        }

        return series;
    }

    private Episode getEpisode(String description, String longDescription, int episodeNumber,
            Image... images) {
        Episode episode = new Episode();

        episode.setDescription(description);
        episode.setLongDescription(longDescription);
        episode.setEpisodeNumber(episodeNumber);
        episode.setImages(ImmutableList.copyOf(images));
        if (images.length > 0) {
            episode.setImage(images[0].getCanonicalUri());
        }

        return episode;
    }

    private BtVodEntry getRow(String guid) {
        BtVodEntry row = new BtVodEntry();
        row.setGuid(guid);
        return row;
    }

    private void checkDescriptionsSource(Content actual, Content expectedSource) {
        assertThat(actual.getDescription(), is(expectedSource.getDescription()));
        assertThat(actual.getLongDescription(), is(expectedSource.getLongDescription()));
    }

    private void checkDescriptionsSource(Content actual, BtVodCollection expectedSource) {
        assertThat(actual.getDescription(), is(expectedSource.getDescription()));
        assertThat(actual.getLongDescription(), is(expectedSource.getLongDescription()));
    }

    private void checkImagesSource(Content actual, Content expectedSource) {
        assertThat(actual.getImages(), is(expectedSource.getImages()));
        assertThat(actual.getImage(), is(expectedSource.getImage()));
    }

    private void checkImagesSource(Content actual, BtVodCollection expectedSource) {
        assertThat(actual.getImages(), is(expectedSource.getImages()));
        assertThat(actual.getImage(), is(expectedSource.getImage()));
    }

    private void checkAliasSource(Brand brand, String guid, Alias unrelatedExistingAlias) {
        assertThat(brand.getAliases().size(), is(3));
        assertThat(brand.getAliases().containsAll(ImmutableSet.of(
                unrelatedExistingAlias,
                new Alias(descriptionsGuidAliasNamespace, guid),
                new Alias(imagesGuidAliasNamespace, guid)
        )), is(true));
    }
}
