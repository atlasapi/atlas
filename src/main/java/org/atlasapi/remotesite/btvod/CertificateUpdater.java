package org.atlasapi.remotesite.btvod;

import org.atlasapi.media.entity.Content;

import com.google.common.collect.Sets;

public class CertificateUpdater {

    public void updateCertificates(Content target, Content source) {
        target.setCertificates(Sets.union(
                target.getCertificates(),
                source.getCertificates()
        ));
    }
}
