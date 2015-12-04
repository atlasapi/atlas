package org.atlasapi.remotesite.btvod;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.Set;

import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodProductPricingPlan;
import org.atlasapi.remotesite.btvod.model.BtVodProductPricingTier;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

@RunWith(MockitoJUnitRunner.class)
public class BtVodVersionsExtractorTest {

    private @Mock BtVodPricingAvailabilityGrouper availabilityGrouper;

    private String uriPrefix = "uriPrefix";
    private String guidAliasNamespace = "aliasNamespace";
    private String idAliasNamespace = "idAliasNamespace";
    private Long btTvServiceId = 1L;
    private Long btTvOtgServiceId = 2L;

    private BtVodVersionsExtractor versionsExtractor;

    @Before
    public void setUp() throws Exception {
        versionsExtractor = new BtVodVersionsExtractor(
                availabilityGrouper,
                uriPrefix,
                guidAliasNamespace,
                idAliasNamespace,
                btTvServiceId,
                btTvOtgServiceId
        );
    }

    @Test
    public void testBlackoutReturnsNoVersions() throws Exception {
        BtVodEntry row = getBlackoutRow();

        Set<Version> versions = versionsExtractor.createVersions(row);

        assertThat(versions.size(), is(0));
    }

    private BtVodEntry getBlackoutRow() {
        DateTime now = DateTime.now().withZone(DateTimeZone.UTC);

        BtVodEntry row = new BtVodEntry();

        BtVodProductPricingPlan plan = new BtVodProductPricingPlan();

        BtVodProductPricingTier tier = new BtVodProductPricingTier();

        tier.setIsBlackout(true);
        tier.setBlackoutType(BtVodVersionsExtractor.PRERELEASE_BLACKOUT_TYPE);
        tier.setProductAbsoluteStart(now.minusHours(1).getMillis());
        tier.setProductAbsoluteEnd(now.plusHours(1).getMillis());

        plan.setProductPricingTiers(ImmutableList.of(tier));

        row.setProductPricingPlan(plan);

        return row;
    }
}