package org.atlasapi.remotesite.bbc.nitro.extract;

import static org.junit.Assert.*;

import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Policy.Network;
import org.atlasapi.media.entity.Policy.Platform;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.TimeMachine;


public class LocationEquivalenceEquivalenceTest {

    private Clock clock = new TimeMachine();
    
    private final LocationEquivalence equiv = new LocationEquivalence();
    
    @Test
    public void testLocationsThatDifferOnAliasesAreNotEquivalent() {
        
        Location loc = new Location();
        loc.addAlias(new Alias("ns", "value1"));
        
        Location different = new Location();
        different.addAlias(new Alias("ns2", "different"));
        
        assertFalse("Locations should not be equivalent if Aliases differ", equiv.doEquivalent(loc, different));
    }

    @Test
    public void testLocationsThatDifferOnAliasUrlsAreNotEquivalent() {
        
        Location loc = new Location();
        loc.addAliasUrl("alias");
        
        Location different = new Location();
        different.addAliasUrl("different_alias");
        
        assertFalse("Locations should not be equivalent if URL Aliases differ", equiv.doEquivalent(loc, different));
    }

    @Test
    public void testLocationsThatDifferOnEquivalentsAreNotEquivalent() {
        
        Location loc = new Location();
        loc.addEquivalentTo(createItem("an Item"));
        
        Location different = new Location();
        different.addEquivalentTo(createItem("a different Item"));
        
        assertFalse("Locations should not be equivalent if Equivalents differ", equiv.doEquivalent(loc, different));
    }

    @Test
    public void testLocationsThatDifferOnAvailabilityAreNotEquivalent() {
        
        Location loc = new Location();
        loc.setAvailable(true);
        
        Location different = new Location();
        different.setAvailable(false);
        
        assertFalse("Locations should not be equivalent if availabilities differ", equiv.doEquivalent(loc, different));
    }

    @Test
    public void testLocationsThatDifferOnTransportTypeAreNotEquivalent() {
        
        Location loc = new Location();
        loc.setTransportType(TransportType.BITTORRENT);
        
        Location different = new Location();
        different.setTransportType(TransportType.LINK);
        
        assertFalse("Locations should not be equivalent if Transport Types differ", equiv.doEquivalent(loc, different));
    }

    @Test
    public void testLocationsThatDifferOnUriAreNotEquivalent() {
        
        Location loc = new Location();
        loc.setCanonicalUri("uri");
        
        Location different = new Location();
        different.setCanonicalUri("different uri");
        
        assertFalse("Locations should not be equivalent if uris differ", equiv.doEquivalent(loc, different));
    }

    @Test
    public void testLocationsThatDifferOnAvailabilityStartAreNotEquivalent() {
        
        Location loc = new Location();
        
        Policy policy = new Policy();
        policy.setAvailabilityStart(clock.now());
        
        loc.setPolicy(policy);
        
        Location different = new Location();
        
        Policy diffPolicy = new Policy();
        diffPolicy.setAvailabilityStart(clock.now().minusHours(2));
        
        different.setPolicy(diffPolicy);
        
        assertFalse("Locations should not be equivalent if availability starts differ", equiv.doEquivalent(loc, different));
    }

    @Test
    public void testLocationsThatDifferOnAvailabilityEndAreNotEquivalent() {
        
        Location loc = new Location();
        
        Policy policy = new Policy();
        policy.setAvailabilityEnd(clock.now());
        
        loc.setPolicy(policy);
        
        Location different = new Location();
        
        Policy diffPolicy = new Policy();
        diffPolicy.setAvailabilityEnd(clock.now().minusHours(2));
        
        different.setPolicy(diffPolicy);
        
        assertFalse("Locations should not be equivalent if availability ends differ", equiv.doEquivalent(loc, different));
    }

    @Test
    public void testLocationsThatDifferOnActualAvailabilityStartAreNotEquivalent() {
        
        Location loc = new Location();
        
        Policy policy = new Policy();
        policy.setActualAvailabilityStart(clock.now());
        
        loc.setPolicy(policy);
        
        Location different = new Location();
        
        Policy diffPolicy = new Policy();
        diffPolicy.setActualAvailabilityStart(clock.now().minusHours(2));
        
        different.setPolicy(diffPolicy);
        
        assertFalse("Locations should not be equivalent if actual availability starts differ", equiv.doEquivalent(loc, different));
    }

    @Test
    public void testLocationsThatDifferOnAvailableCountriesAreNotEquivalent() {
        
        Location loc = new Location();
        
        Policy policy = new Policy();
        policy.setAvailableCountries(ImmutableSet.of(Countries.GB));
        
        loc.setPolicy(policy);
        
        Location different = new Location();
        
        Policy diffPolicy = new Policy();
        diffPolicy.setAvailableCountries(ImmutableSet.of(Countries.FR));
        
        different.setPolicy(diffPolicy);
        
        assertFalse("Locations should not be equivalent if available countries differ", equiv.doEquivalent(loc, different));
    }

    @Test
    public void testLocationsThatDifferOnPlatformAreNotEquivalent() {
        
        Location loc = new Location();
        
        Policy policy = new Policy();
        policy.setPlatform(Platform.PC);
        
        loc.setPolicy(policy);
        
        Location different = new Location();
        
        Policy diffPolicy = new Policy();
        diffPolicy.setPlatform(Platform.XBOX);
        
        different.setPolicy(diffPolicy);
        
        assertFalse("Locations should not be equivalent if platforms differ", equiv.doEquivalent(loc, different));
    }

    @Test
    public void testLocationsThatDifferOnNetworkAreNotEquivalent() {
        
        Location loc = new Location();
        
        Policy policy = new Policy();
        policy.setNetwork(Network.THREE_G);
        
        loc.setPolicy(policy);
        
        Location different = new Location();
        
        Policy diffPolicy = new Policy();
        diffPolicy.setNetwork(Network.WIFI);
        
        different.setPolicy(diffPolicy);
        
        assertFalse("Locations should not be equivalent if networks differ", equiv.doEquivalent(loc, different));
    }
    
    @Test
    public void testLocationsThatDifferOnLastUpdatedAreEquivalent() {
        DateTime lastUpdated = clock.now();
        
        Location loc = new Location();
        
        loc.addAlias(new Alias("ns", "value1"));
        loc.addAliasUrl("alias");
        loc.setAvailable(true);
        loc.setTransportType(TransportType.LINK);
        loc.setCanonicalUri("uri");
        loc.setLastUpdated(lastUpdated);
        
        Policy policy = new Policy();
        
        policy.setAvailabilityStart(clock.now());
        policy.setAvailabilityStart(clock.now());
        policy.setActualAvailabilityStart(clock.now().plusMinutes(3));
        policy.setAvailabilityEnd(clock.now().plusDays(10));
        policy.setAvailableCountries(ImmutableSet.of(Countries.GB));
        policy.setPlatform(Platform.PC);
        policy.setNetwork(Network.THREE_G);
        
        loc.setPolicy(policy);
        
        Location different = new Location();
        
        different.addAlias(new Alias("ns", "value1"));
        different.addAliasUrl("alias");
        different.setAvailable(true);
        different.setTransportType(TransportType.LINK);
        different.setCanonicalUri("uri");
        different.setLastUpdated(lastUpdated.plusSeconds(73));
        
        Policy diffPolicy = new Policy();
        
        diffPolicy.setAvailabilityStart(clock.now());
        diffPolicy.setAvailabilityStart(clock.now());
        diffPolicy.setActualAvailabilityStart(clock.now().plusMinutes(3));
        diffPolicy.setAvailabilityEnd(clock.now().plusDays(10));
        diffPolicy.setAvailableCountries(ImmutableSet.of(Countries.GB));
        diffPolicy.setPlatform(Platform.PC);
        diffPolicy.setNetwork(Network.THREE_G);
        
        different.setPolicy(diffPolicy);
        
        assertTrue("Locations should be equivalent if only LastUpdated timestamps differ", equiv.doEquivalent(loc, different));
    }

    private Described createItem(String uri) {
        return new Film(uri, "curie", Publisher.METABROADCAST);
    }
}
