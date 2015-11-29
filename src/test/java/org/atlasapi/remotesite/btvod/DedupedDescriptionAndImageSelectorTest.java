package org.atlasapi.remotesite.btvod;

import static org.atlasapi.media.entity.Policy.RevenueContract.PAY_TO_BUY;
import static org.atlasapi.media.entity.Policy.RevenueContract.PAY_TO_RENT;
import static org.atlasapi.media.entity.Policy.RevenueContract.SUBSCRIPTION;
import static org.atlasapi.media.entity.Quality.FOUR_K;
import static org.atlasapi.media.entity.Quality.HD;
import static org.atlasapi.media.entity.Quality.SD;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Quality;
import org.atlasapi.media.entity.Version;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class DedupedDescriptionAndImageSelectorTest {

    private DedupedDescriptionAndImageSelector selector;

    @Before
    public void setUp() throws Exception {
        selector = new DedupedDescriptionAndImageSelector();
    }

    @Test
    public void testShouldUpdateDescriptionsAndImagesForHigherPriorityQuality() throws Exception {
        boolean decision = selector.shouldUpdateDescriptionsAndImages(
                getVersions(ImmutableMap.of(SD, PAY_TO_RENT)),
                getVersions(ImmutableMap.of(HD, SUBSCRIPTION))
        );
        
        assertThat(decision, is(true));
    }

    @Test
    public void testShouldNotUpdateDescriptionsAndImagesForLowerPriorityQuality() throws Exception {
        boolean decision = selector.shouldUpdateDescriptionsAndImages(
                getVersions(ImmutableMap.of(HD, SUBSCRIPTION)),
                getVersions(ImmutableMap.of(SD, PAY_TO_RENT))
        );

        assertThat(decision, is(false));
    }

    @Test
    public void testShouldUpdateDescriptionsAndImagesForHigherPriorityContractIfQualityIsSame() 
            throws Exception {
        boolean decision = selector.shouldUpdateDescriptionsAndImages(
                getVersions(ImmutableMap.of(HD, SUBSCRIPTION)),
                getVersions(ImmutableMap.of(HD, PAY_TO_RENT))
        );

        assertThat(decision, is(true));
    }

    @Test
    public void testShouldNotUpdateDescriptionsAndImagesForLowerPriorityContractIfQualityIsSame() 
            throws Exception {
        boolean decision = selector.shouldUpdateDescriptionsAndImages(
                getVersions(ImmutableMap.of(HD, PAY_TO_BUY)),
                getVersions(ImmutableMap.of(HD, PAY_TO_RENT))
        );

        assertThat(decision, is(false));
    }

    @Test
    public void testShouldUpdateDescriptionsAndImagesIfNoExistingVersions() throws Exception {
        boolean decision = selector.shouldUpdateDescriptionsAndImages(
                getVersions(ImmutableMap.of(HD, PAY_TO_RENT)),
                ImmutableSet.<Version>of()
        );
        
        assertThat(decision, is(true));
    }

    @Test
    public void testShouldNotUpdateDescriptionsAndImagesIfNoCurrentVersions() throws Exception {
        boolean decision = selector.shouldUpdateDescriptionsAndImages(
                ImmutableSet.<Version>of(),
                getVersions(ImmutableMap.of(HD, PAY_TO_BUY))
        );
        
        assertThat(decision, is(false));
    }

    @Test
    public void testPickBestVersionTypeWhenThereAreMultiple() throws Exception {
        boolean decision = selector.shouldUpdateDescriptionsAndImages(
                getVersions(ImmutableMap.<Quality, Policy.RevenueContract>builder()
                        .put(FOUR_K, SUBSCRIPTION)
                        .put(SD, PAY_TO_RENT)
                        .build()
                ),
                getVersions(ImmutableMap.<Quality, Policy.RevenueContract>builder()
                        .put(HD, SUBSCRIPTION)
                        .put(SD, PAY_TO_BUY)
                        .build())
        );

        assertThat(decision, is(true));
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

        boolean decision = selector.shouldUpdateDescriptionsAndImages(
                ImmutableSet.of(currentVersion),
                getVersions(ImmutableMap.of(SD, PAY_TO_BUY))
        );

        assertThat(decision, is(true));
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

        boolean decision = selector.shouldUpdateDescriptionsAndImages(
                ImmutableSet.of(currentVersion),
                getVersions(ImmutableMap.of(SD, PAY_TO_BUY))
        );

        assertThat(decision, is(true));
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
}