package org.atlasapi.remotesite.bbc.nitro.extract;

import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;

import com.google.common.base.Equivalence;
import com.google.common.base.Objects;

final class LocationEquivalence extends Equivalence<Location> {

    @Override
    protected boolean doEquivalent(Location a, Location b) {
        return Objects.equal(a.getAliases(), b.getAliases())
                && Objects.equal(a.getAliasUrls(), b.getAliasUrls())
                && Objects.equal(a.getEquivalentTo(), b.getEquivalentTo())
                && a.getAvailable() == b.getAvailable()
                && Objects.equal(a.getTransportType(), b.getTransportType())
                && Objects.equal(a.getCanonicalUri(), b.getCanonicalUri())
                && equivalent(a.getPolicy(), b.getPolicy());
    }

    private boolean equivalent(Policy a, Policy b) {
        return Objects.equal(a.getAvailabilityStart(), b.getAvailabilityStart())
                && Objects.equal(a.getAvailabilityEnd(), b.getAvailabilityEnd())
                && Objects.equal(a.getActualAvailabilityStart(), b.getActualAvailabilityStart())
                && Objects.equal(a.getAvailableCountries(), b.getAvailableCountries())
                && Objects.equal(a.getPlatform(), b.getPlatform())
                && Objects.equal(a.getNetwork(), b.getNetwork());
    }

    @Override
    protected int doHash(Location loc) {
        return Objects.hashCode(
                loc.getAliases(), 
                loc.getAliasUrls(), 
                loc.getEquivalentTo(), 
                loc.getAvailable(), 
                loc.getTransportType(), 
                loc.getCanonicalUri(), 
                hashCode(loc.getPolicy())
        );
    }

    private int hashCode(Policy policy) {
        return Objects.hashCode(
                policy.getAvailabilityStart(), 
                policy.getAvailabilityEnd(), 
                policy.getActualAvailabilityStart(), 
                policy.getAvailableCountries(), 
                policy.getPlatform(), 
                policy.getNetwork()
        );
    }
}