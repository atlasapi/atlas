package org.atlasapi.remotesite.bbc.nitro.extract;

import static org.junit.Assert.*;

import java.util.Set;

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

import com.google.common.base.Equivalence;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.TimeMachine;


public class LocationWrapperSetEquivalenceTest {

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
        
        Set<Equivalence.Wrapper<Location>> locations = ImmutableSet.of(
                equiv.wrap(loc),
                equiv.wrap(different)
                );
        assertEquals("Locations should not be equivalent if Aliases differ", 2, locations.size());
    }

    @Test
    public void testLocationsThatDifferOnAliasUrlsAreNotHashCodeEqual() {
        
        Location loc = new Location();
        loc.setPolicy(new Policy());
        loc.addAliasUrl("alias");
        
        Location different = new Location();
        different.setPolicy(new Policy());
        different.addAliasUrl("different_alias");
        
        Set<Equivalence.Wrapper<Location>> locations = ImmutableSet.of(
                equiv.wrap(loc),
                equiv.wrap(different)
                );
        assertEquals("Locations should not be equivalent if URL aliases differ", 2, locations.size());
    }

    @Test
    public void testLocationsThatDifferOnEquivalentsAreNotHashCodeEqual() {
        
        Location loc = new Location();
        loc.setPolicy(new Policy());
        loc.addEquivalentTo(createItem("an Item"));
        
        Location different = new Location();
        different.setPolicy(new Policy());
        different.addEquivalentTo(createItem("a different Item"));
        
        Set<Equivalence.Wrapper<Location>> locations = ImmutableSet.of(
                equiv.wrap(loc),
                equiv.wrap(different)
                );
        assertEquals("Locations should not be equivalent if equivalents differ", 2, locations.size());
    }

    @Test
    public void testLocationsThatDifferOnAvailabilityAreNotHashCodeEqual() {
        
        Location loc = new Location();
        loc.setPolicy(new Policy());
        loc.setAvailable(true);
        
        Location different = new Location();
        different.setPolicy(new Policy());
        different.setAvailable(false);
        
        Set<Equivalence.Wrapper<Location>> locations = ImmutableSet.of(
                equiv.wrap(loc),
                equiv.wrap(different)
                );
        assertEquals("Locations should not be equivalent if availabilities differ", 2, locations.size());
    }

    @Test
    public void testLocationsThatDifferOnTransportTypeAreNotHashCodeEqual() {
        
        Location loc = new Location();
        loc.setPolicy(new Policy());
        loc.setTransportType(TransportType.BITTORRENT);
        
        Location different = new Location();
        different.setPolicy(new Policy());
        different.setTransportType(TransportType.LINK);
        
        Set<Equivalence.Wrapper<Location>> locations = ImmutableSet.of(
                equiv.wrap(loc),
                equiv.wrap(different)
                );
        assertEquals("Locations should not be equivalent if transport types differ", 2, locations.size());
    }

    @Test
    public void testLocationsThatDifferOnUriAreNotHashCodeEqual() {
        
        Location loc = new Location();
        loc.setPolicy(new Policy());
        loc.setCanonicalUri("uri");
        
        Location different = new Location();
        different.setPolicy(new Policy());
        different.setCanonicalUri("different uri");
        
        Set<Equivalence.Wrapper<Location>> locations = ImmutableSet.of(
                equiv.wrap(loc),
                equiv.wrap(different)
                );
        assertEquals("Locations should not be equivalent if uris differ", 2, locations.size());
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
        
        Set<Equivalence.Wrapper<Location>> locations = ImmutableSet.of(
                equiv.wrap(loc),
                equiv.wrap(different)
                );
        assertEquals("Locations should not be equivalent if availability starts differ", 2, locations.size());
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
        
        Set<Equivalence.Wrapper<Location>> locations = ImmutableSet.of(
                equiv.wrap(loc),
                equiv.wrap(different)
                );
        assertEquals("Locations should not be equivalent if availability ends differ", 2, locations.size());
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
        
        Set<Equivalence.Wrapper<Location>> locations = ImmutableSet.of(
                equiv.wrap(loc),
                equiv.wrap(different)
                );
        assertEquals("Locations should not be equivalent if actual availability starts differ", 2, locations.size());
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
        
        Set<Equivalence.Wrapper<Location>> locations = ImmutableSet.of(
                equiv.wrap(loc),
                equiv.wrap(different)
                );
        assertEquals("Locations should not be equivalent if available countries differ", 2, locations.size());
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
        
        Set<Equivalence.Wrapper<Location>> locations = ImmutableSet.of(
                equiv.wrap(loc),
                equiv.wrap(different)
                );
        assertEquals("Locations should not be equivalent if platforms differ", 2, locations.size());
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
        
        Set<Equivalence.Wrapper<Location>> locations = ImmutableSet.of(
                equiv.wrap(loc),
                equiv.wrap(different)
                );
        assertEquals("Locations should not be equivalent if networks differ", 2, locations.size());
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
        
        Set<Equivalence.Wrapper<Location>> locations = ImmutableSet.of(
                equiv.wrap(loc),
                equiv.wrap(different)
                );
        assertEquals("Locations should be equivalent if only LastUpdated timestamps differ", 1, locations.size());
    }

    private Described createItem(String uri) {
        return new Film(uri, "curie", Publisher.METABROADCAST);
    }
}
