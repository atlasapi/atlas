package org.atlasapi.remotesite.btvod;

import static org.atlasapi.media.entity.Policy.RevenueContract.PAY_TO_BUY;
import static org.atlasapi.media.entity.Policy.RevenueContract.PAY_TO_RENT;
import static org.atlasapi.media.entity.Policy.RevenueContract.SUBSCRIPTION;
import static org.atlasapi.media.entity.Quality.FOUR_K;
import static org.atlasapi.media.entity.Quality.HD;
import static org.atlasapi.media.entity.Quality.SD;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Quality;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class DedupedDescriptionAndImageUpdaterTest {

    private DedupedDescriptionAndImageUpdater updater;

    private Content target;
    private BtVodEntry firstRow;
    private BtVodEntry secondRow;
    private Set<Image> firstImages;
    private Set<Image> secondImages;

    @Before
    public void setUp() throws Exception {
        updater = new DedupedDescriptionAndImageUpdater();

        target = new Item();
        target.setCanonicalUri("uri");

        firstRow = new BtVodEntry();
        firstRow.setDescription("a");
        firstRow.setProductLongDescription("aL");

        secondRow = new BtVodEntry();
        secondRow.setDescription("b");
        secondRow.setProductLongDescription("bL");

        firstImages = ImmutableSet.of(new Image("imageA"));
        secondImages = ImmutableSet.of(new Image("imageB"));
    }

    @Test
    public void testShouldNotUpdateIfSourceHasNoUsefulValues() throws Exception {
        updater.updateDescriptionsAndImages(
                target, new BtVodEntry(), ImmutableSet.<Image>of(),
                getVersions(ImmutableMap.of(HD, SUBSCRIPTION))
        );

        assertThat(target.getDescription(), is(CoreMatchers.<String>nullValue()));
        assertThat(target.getLongDescription(), is(CoreMatchers.<String>nullValue()));
        assertThat(target.getImages(), is(CoreMatchers.<Set<Image>>nullValue()));
    }

    @Test
    public void testShouldAlwaysUpdateIfTargetHasNotBeenSeenBefore() throws Exception {
        target.setDescription("desc");

        updater.updateDescriptionsAndImages(
                target, firstRow, firstImages, getVersions(ImmutableMap.of(HD, SUBSCRIPTION))
        );

        checkChoice(firstRow, firstImages);
    }

    @Test
    public void testShouldUpdateDescriptionsAndImagesForHigherPriorityQuality() throws Exception {
        updater.updateDescriptionsAndImages(
                target, firstRow, firstImages, getVersions(ImmutableMap.of(HD, SUBSCRIPTION))
        );
        updater.updateDescriptionsAndImages(
                target, secondRow, secondImages, getVersions(ImmutableMap.of(SD, PAY_TO_RENT))
        );

        checkChoice(secondRow, secondImages);
    }

    @Test
    public void testShouldNotUpdateDescriptionsAndImagesForLowerPriorityQuality() throws Exception {
        updater.updateDescriptionsAndImages(
                target, firstRow, firstImages, getVersions(ImmutableMap.of(SD, PAY_TO_RENT))
        );
        updater.updateDescriptionsAndImages(
                target, secondRow, secondImages, getVersions(ImmutableMap.of(HD, SUBSCRIPTION))
        );

        checkChoice(firstRow, firstImages);
    }

    @Test
    public void testShouldUpdateDescriptionsAndImagesForHigherPriorityContractIfQualityIsSame()
            throws Exception {
        updater.updateDescriptionsAndImages(
                target, firstRow, firstImages, getVersions(ImmutableMap.of(HD, PAY_TO_RENT))
        );
        updater.updateDescriptionsAndImages(
                target, secondRow, secondImages, getVersions(ImmutableMap.of(HD, SUBSCRIPTION))
        );

        checkChoice(secondRow, secondImages);
    }

    @Test
    public void testShouldNotUpdateDescriptionsAndImagesForLowerPriorityContractIfQualityIsSame()
            throws Exception {
        updater.updateDescriptionsAndImages(
                target, firstRow, firstImages, getVersions(ImmutableMap.of(HD, PAY_TO_RENT))
        );
        updater.updateDescriptionsAndImages(
                target, secondRow, secondImages, getVersions(ImmutableMap.of(HD, PAY_TO_BUY))
        );

        checkChoice(firstRow, firstImages);
    }

    @Test
    public void testShouldUpdateDescriptionsAndImagesIfNoExistingVersions() throws Exception {
        updater.updateDescriptionsAndImages(
                target, firstRow, firstImages, getVersions(ImmutableMap.of(HD, PAY_TO_RENT))
        );

        checkChoice(firstRow, firstImages);
    }

    @Test
    public void testShouldNotUpdateDescriptionsAndImagesIfNoCurrentVersions() throws Exception {
        updater.updateDescriptionsAndImages(
                target, firstRow, firstImages,
                getVersions(ImmutableMap.<Quality, Policy.RevenueContract>of())
        );

        assertThat(target.getDescription(), is(nullValue()));
        assertThat(target.getLongDescription(), is(nullValue()));
        assertThat(target.getImages(), is(nullValue()));
    }

    @Test
    public void testPickBestVersionTypeWhenThereAreMultiple() throws Exception {
        updater.updateDescriptionsAndImages(
                target, firstRow, firstImages,
                getVersions(ImmutableMap.<Quality, Policy.RevenueContract>builder()
                        .put(HD, SUBSCRIPTION)
                        .put(SD, PAY_TO_BUY)
                        .build())
        );
        updater.updateDescriptionsAndImages(
                target, secondRow, secondImages,
                getVersions(ImmutableMap.<Quality, Policy.RevenueContract>builder()
                        .put(FOUR_K, SUBSCRIPTION)
                        .put(SD, PAY_TO_RENT)
                        .build())
        );

        checkChoice(secondRow, secondImages);
    }

    @Test
    public void testPickBestOptionsWhenSetsInsideVersionHaveCardinalityGreaterThanOne()
            throws Exception {
        Encoding currentEncodingA = new Encoding();
        currentEncodingA.setQuality(FOUR_K);
        currentEncodingA.setAvailableAt(ImmutableSet.of(
                getLocation(PAY_TO_BUY),
                getLocation(SUBSCRIPTION)
        ));

        Encoding currentEncodingB = new Encoding();
        currentEncodingB.setQuality(SD);
        currentEncodingB.setAvailableAt(ImmutableSet.of(
                getLocation(PAY_TO_BUY),
                getLocation(PAY_TO_RENT)
        ));

        Version currentVersion = new Version();
        currentVersion.setManifestedAs(ImmutableSet.of(currentEncodingA, currentEncodingB));

        updater.updateDescriptionsAndImages(
                target, firstRow, firstImages, getVersions(ImmutableMap.of(SD, PAY_TO_BUY))
        );
        updater.updateDescriptionsAndImages(
                target, secondRow, secondImages, ImmutableSet.of(currentVersion)
        );

        checkChoice(secondRow, secondImages);
    }

    @Test
    public void testPickBestOptionsWhenSomeFieldsAreNull() throws Exception {
        Encoding currentEncodingA = new Encoding();
        currentEncodingA.setQuality(FOUR_K);
        currentEncodingA.setAvailableAt(Sets.newHashSet(
                getLocation(PAY_TO_BUY),
                getLocation(SUBSCRIPTION),
                null
        ));

        Encoding currentEncodingB = new Encoding();
        currentEncodingB.setQuality(SD);
        currentEncodingB.setAvailableAt(Sets.newHashSet(
                getLocation(null),
                null,
                getLocation(SUBSCRIPTION)
        ));

        Encoding currentEncodingC = new Encoding();
        currentEncodingC.setQuality(null);
        currentEncodingC.setAvailableAt(Sets.newHashSet(getLocation(SUBSCRIPTION)));

        Version currentVersion = new Version();
        currentVersion.setManifestedAs(Sets.newHashSet(
                currentEncodingA, null, currentEncodingB, currentEncodingC
        ));

        updater.updateDescriptionsAndImages(
                target, firstRow, firstImages, getVersions(ImmutableMap.of(SD, PAY_TO_BUY))
        );
        updater.updateDescriptionsAndImages(
                target, secondRow, secondImages, ImmutableSet.of(currentVersion)
        );

        checkChoice(secondRow, secondImages);
    }

    private Set<Version> getVersions(Map<Quality, Policy.RevenueContract> qualityContractMap) {
        ImmutableSet.Builder<Version> builder = ImmutableSet.builder();

        for (Map.Entry<Quality, Policy.RevenueContract> entries : qualityContractMap.entrySet()) {
            Version version = new Version();

            version.addManifestedAs(getEncoding(entries.getKey(), entries.getValue()));

            builder.add(version);
        }

        return builder.build();
    }

    private Encoding getEncoding(Quality quality, Policy.RevenueContract contract) {
        Encoding encoding = new Encoding();

        encoding.setQuality(quality);
        encoding.setAvailableAt(ImmutableSet.of(getLocation(contract)));

        return encoding;
    }

    private Location getLocation(Policy.RevenueContract contract) {
        Location location = new Location();

        Policy policy = new Policy();
        policy.setRevenueContract(contract);

        location.setPolicy(policy);

        return location;
    }

    private void checkChoice(BtVodEntry expectedRow, Set<Image> expectedImages) {
        assertThat(target.getDescription(), is(expectedRow.getDescription()));
        assertThat(target.getLongDescription(), is(expectedRow.getProductLongDescription()));
        assertThat(target.getImages(), is(expectedImages));
    }
}