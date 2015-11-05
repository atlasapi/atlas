package org.atlasapi.remotesite.btvod;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.media.entity.Certificate;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.intl.Countries;

@RunWith(MockitoJUnitRunner.class)
public class CertificateUpdaterTest {

    private CertificateUpdater certificateUpdater;

    private Content target;
    private Content source;

    @Before
    public void setUp() throws Exception {
        certificateUpdater = new CertificateUpdater();

        target = new Item();
        source = new Item();
    }

    @Test
    public void testUpdateCertificatesMergesSourceAndTargetCertificates() throws Exception {
        Certificate certificateA = new Certificate("15", Countries.GB);
        Certificate certificateB = new Certificate("15", Countries.FR);
        Certificate certificateC = new Certificate("18", Countries.GB);

        target.setCertificates(ImmutableSet.of(certificateA, certificateB));
        source.setCertificates(ImmutableSet.of(certificateC));

        certificateUpdater.updateCertificates(target, source);

        verifyExpectedCertificatesInContent(target, certificateA, certificateB, certificateC);
    }

    @Test
    public void testUpdateCertificatesDedupesThem() throws Exception {
        Certificate certificateA = new Certificate("15", Countries.GB);
        Certificate certificateB = new Certificate("15", Countries.FR);
        Certificate certificateC = new Certificate("18", Countries.GB);
        Certificate certificateD = new Certificate("15", Countries.GB);

        target.setCertificates(ImmutableSet.of(certificateA, certificateB));
        source.setCertificates(ImmutableSet.of(certificateC, certificateD));

        certificateUpdater.updateCertificates(target, source);

        verifyExpectedCertificatesInContent(target, certificateA, certificateB, certificateC);
    }

    private void verifyExpectedCertificatesInContent(Content content, Certificate... certificates) {
        assertThat(content.getCertificates().size(), is(certificates.length));

        assertThat(content.getCertificates().containsAll(ImmutableList.copyOf(certificates)),
                is(true));
    }
}