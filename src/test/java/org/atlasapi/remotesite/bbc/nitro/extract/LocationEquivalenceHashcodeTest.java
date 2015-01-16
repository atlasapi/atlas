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


public class LocationEquivalenceHashcodeTest {

    private Clock clock = new TimeMachine();
    
    private final LocationEquivalence equiv = new LocationEquivalence();
    
    @Test
    public void testLocationsThatDifferOnAliasesAreNotHashCodeEqual() {
        
        Location loc = new Location();
        loc.setPolicy(new Policy());
        loc.addAlias(new Alias("ns", "value1"));
        
        Location different = new Location();
        different.setPolicy(new Policy());
        different.addAlias(new Alias("ns2", "different"));
        
        assertNotEquals("Locations should not be hash code equal if Aliases differ", equiv.doHash(loc), equiv.doHash(different));
    }

    @Test
    public void testLocationsThatDifferOnAliasUrlsAreNotHashCodeEqual() {
        
        Location loc = new Location();
        loc.setPolicy(new Policy());
        loc.addAliasUrl("alias");
        
        Location different = new Location();
        different.setPolicy(new Policy());
        different.addAliasUrl("different_alias");
        
        assertNotEquals("Locations should not be hash code equal if URL Aliases differ", equiv.doHash(loc), equiv.doHash(different));
    }

    @Test
    public void testLocationsThatDifferOnEquivalentsAreNotHashCodeEqual() {
        
        Location loc = new Location();
        loc.setPolicy(new Policy());
        loc.addEquivalentTo(createItem("an Item"));
        
        Location different = new Location();
        different.setPolicy(new Policy());
        different.addEquivalentTo(createItem("a different Item"));
        
        assertNotEquals("Locations should not be hash code equal if Equivalents differ", equiv.doHash(loc), equiv.doHash(different));
    }

    @Test
    public void testLocationsThatDifferOnAvailabilityAreNotHashCodeEqual() {
        
        Location loc = new Location();
        loc.setPolicy(new Policy());
        loc.setAvailable(true);
        
        Location different = new Location();
        different.setPolicy(new Policy());
        different.setAvailable(false);
        
        assertNotEquals("Locations should not be hash code equal if availabilities differ", equiv.doHash(loc), equiv.doHash(different));
    }

    @Test
    public void testLocationsThatDifferOnTransportTypeAreNotHashCodeEqual() {
        
        Location loc = new Location();
        loc.setPolicy(new Policy());
        loc.setTransportType(TransportType.BITTORRENT);
        
        Location different = new Location();
        different.setPolicy(new Policy());
        different.setTransportType(TransportType.LINK);
        
        assertNotEquals("Locations should not be hash code equal if Transport Types differ", equiv.doHash(loc), equiv.doHash(different));
    }

    @Test
    public void testLocationsThatDifferOnUriAreNotHashCodeEqual() {
        
        Location loc = new Location();
        loc.setPolicy(new Policy());
        loc.setCanonicalUri("uri");
        
        Location different = new Location();
        different.setPolicy(new Policy());
        different.setCanonicalUri("different uri");
        
        assertNotEquals("Locations should not be hash code equal if uris differ", equiv.doHash(loc), equiv.doHash(different));
    }

    @Test
    public void testLocationsThatDifferOnAvailabilityStartAreNotHashCodeEqual() {
        
        Location loc = new Location();
        
        Policy policy = new Policy();
        policy.setAvailabilityStart(clock.now());
        
        loc.setPolicy(policy);
        
        Location different = new Location();
        
        Policy diffPolicy = new Policy();
        diffPolicy.setAvailabilityStart(clock.now().minusHours(2));
        
        different.setPolicy(diffPolicy);
        
        assertNotEquals("Locations should not be hash code equal if availability starts differ", equiv.doHash(loc), equiv.doHash(different));
    }

    @Test
    public void testLocationsThatDifferOnAvailabilityEndAreNotHashCodeEqual() {
        
        Location loc = new Location();
        
        Policy policy = new Policy();
        policy.setAvailabilityEnd(clock.now());
        
        loc.setPolicy(policy);
        
        Location different = new Location();
        
        Policy diffPolicy = new Policy();
        diffPolicy.setAvailabilityEnd(clock.now().minusHours(2));
        
        different.setPolicy(diffPolicy);
        
        assertNotEquals("Locations should not be hash code equal if availability ends differ", equiv.doHash(loc), equiv.doHash(different));
    }

    @Test
    public void testLocationsThatDifferOnActualAvailabilityStartAreNotHashCodeEqual() {
        
        Location loc = new Location();
        
        Policy policy = new Policy();
        policy.setActualAvailabilityStart(clock.now());
        
        loc.setPolicy(policy);
        
        Location different = new Location();
        
        Policy diffPolicy = new Policy();
        diffPolicy.setActualAvailabilityStart(clock.now().minusHours(2));
        
        different.setPolicy(diffPolicy);
        
        assertNotEquals("Locations should not be hash code equal if actual availability starts differ", equiv.doHash(loc), equiv.doHash(different));
    }

    @Test
    public void testLocationsThatDifferOnAvailableCountriesAreNotHashCodeEqual() {
        
        Location loc = new Location();
        
        Policy policy = new Policy();
        policy.setAvailableCountries(ImmutableSet.of(Countries.GB));
        
        loc.setPolicy(policy);
        
        Location different = new Location();
        
        Policy diffPolicy = new Policy();
        diffPolicy.setAvailableCountries(ImmutableSet.of(Countries.FR));
        
        different.setPolicy(diffPolicy);
        
        assertNotEquals("Locations should not be hash code equal if available countries differ", equiv.doHash(loc), equiv.doHash(different));
    }

    @Test
    public void testLocationsThatDifferOnPlatformAreNotHashCodeEqual() {
        
        Location loc = new Location();
        
        Policy policy = new Policy();
        policy.setPlatform(Platform.PC);
        
        loc.setPolicy(policy);
        
        Location different = new Location();
        
        Policy diffPolicy = new Policy();
        diffPolicy.setPlatform(Platform.XBOX);
        
        different.setPolicy(diffPolicy);
        
        assertNotEquals("Locations should not be hash code equal if platforms differ", equiv.doHash(loc), equiv.doHash(different));
    }

    @Test
    public void testLocationsThatDifferOnNetworkAreNotHashCodeEqual() {
        
        Location loc = new Location();
        
        Policy policy = new Policy();
        policy.setNetwork(Network.THREE_G);
        
        loc.setPolicy(policy);
        
        Location different = new Location();
        
        Policy diffPolicy = new Policy();
        diffPolicy.setNetwork(Network.WIFI);
        
        different.setPolicy(diffPolicy);
        
        assertNotEquals("Locations should not be hash code equal if networks differ", equiv.doHash(loc), equiv.doHash(different));
    }
    
    @Test
    public void testLocationsThatDifferOnLastUpdatedAreHashCodeEqual() {
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
        
        assertEquals("Locations should be hash code equal if only LastUpdated timestamps differ", equiv.doHash(loc), equiv.doHash(different));
    }

    private Described createItem(String uri) {
        return new Film(uri, "curie", Publisher.METABROADCAST);
    }
}
