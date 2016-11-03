package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.media.TransportSubType;
import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Policy.Platform;
import org.atlasapi.media.entity.Policy.RevenueContract;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.ContentExtractor;

import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;

import com.google.api.client.repackaged.com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import org.jdom.Element;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class C4AtomEntryVersionExtractor implements ContentExtractor<C4VersionData, Version> {
    
    private static final String VIDEOID_PLACEHOLDER = "%VIDEOID%";
    private static final String DC_AGE_RATING = "dc:relation.AgeRating";
    private static final String DC_GUIDANCE = "dc:relation.Guidance";
    private static final Pattern CLIP_ID_PATTERN = Pattern.compile("tag:pmlsc\\.channel4\\.com,\\d+:clip\\/(.+)");
    private static final String EMBED_CODE = "<script language=\"JavaScript\" type=\"text/javascript\" src=\"http://admin.brightcove.com/js/BrightcoveExperiences.js\"></script>"
                                            + "<object id=\"myExperience" + VIDEOID_PLACEHOLDER + "\" class=\"BrightcoveExperience\">"
                                            + "<param name=\"bgcolor\" value=\"#FFFFFF\" />"
                                            + "<param name=\"width\" value=\"640\" />"
                                            + "<param name=\"height\" value=\"398\" />"
                                            + "<param name=\"playerID\" value=\"2838875326001\" />"
                                            + "<param name=\"playerKey\" value=\"AQ~~,AAACd_aq9fk~,IpkmYw1RzNVBilrY_Ykl5eOHSxsiKXpd\" />"
                                            + "<param name=\"isVid\" value=\"true\" />"
                                            + "<param name=\"isUI\" value=\"true\" />"
                                            + "<param name=\"dynamicStreaming\" value=\"true\" />"
                                            + "<param name=\"@videoPlayer\" value=\"" + VIDEOID_PLACEHOLDER + "\" />"
                                            + "</object>";
    
    private final Optional<Platform> platform;
    private final C4LocationPolicyIds locationPolicyIds;
    private final boolean createIosBrandLocations;
    
    public C4AtomEntryVersionExtractor(Optional<Platform> platform, C4LocationPolicyIds locationPolicyIds,
            boolean createIosBrandLocations) {
        this.platform = checkNotNull(platform);
        this.locationPolicyIds = checkNotNull(locationPolicyIds);
        this.createIosBrandLocations = createIosBrandLocations;
    }
    
    @Override
    public Version extract(C4VersionData data) {

        Version version = new Version();
        version.setLastUpdated(data.getLastUpdated());
        Duration duration = C4AtomApi.durationFrom(data.getLookup());
        
        if (duration != null) {
            version.setDuration(duration);
        }
        
        Restriction restriction = extractRestriction(data);
        if (restriction != null) {
            version.setRestriction(restriction);
        }
        
        Encoding encoding = new Encoding();
        
        Location location = extractLocation(data.getUri(), data, 
                locationPolicyIds.getPlayerId(), locationPolicyIds.getWebServiceId());
        if (location.getPolicy() != null) {
            encoding.addAvailableAt(location);
        }
        
        if (createIosBrandLocations) {
            Location iOsLocation = extractLocation(
                    C4AtomApi.iOsUriFromPcUri(data.getUri(), data.getLookup()),
                    data,
                    locationPolicyIds.getPlayerId(),
                    locationPolicyIds.getIOsServiceId()
            );
            if (iOsLocation.getPolicy() != null) {
                encoding.addAvailableAt(iOsLocation);
            }
        }
        
        Matcher matcher = CLIP_ID_PATTERN.matcher(data.getId());
        if (matcher.matches()) {
            Location embedLocation = embedLocation(matcher.group(1), location);
            encoding.addAvailableAt(embedLocation);
        }
        
        version.addManifestedAs(encoding);
                
        return version.getBroadcasts().isEmpty() && version.getManifestedAs().isEmpty() ? null : version;
    }

    private Restriction extractRestriction(C4VersionData data) {
        Map<String, String> lookup = data.getLookup();
        Integer ageRating = Ints.tryParse(Strings.nullToEmpty(lookup.get(DC_AGE_RATING)));
        String guidance = lookup.get(DC_GUIDANCE);
        
        Restriction restriction = null;
        if (Objects.firstNonNull(ageRating, 0) > 0 && guidance != null) {
            restriction = Restriction.from(ageRating, guidance);
            restriction.setLastUpdated(data.getLastUpdated());
        } else if (guidance != null){
            restriction = Restriction.from(guidance);
            restriction.setLastUpdated(data.getLastUpdated());
        }
        return restriction;
    }

    private static final Pattern AVAILABILTY_RANGE_PATTERN = Pattern.compile("start=(.*); end=(.*); scheme=W3C-DTF");
    
    private Location extractLocation(String uri, C4VersionData data, Long playerId, Long serviceId) {
        Location location = new Location();
        location.setUri(uri);
        location.setLastUpdated(data.getLastUpdated());
        if(data.getId() != null) { 
            location.addAliasUrl(data.getId().replace("tag:pmlsc", "tag:www"));
        }
        location.setTransportType(TransportType.LINK);
        
        Policy policy = extractPolicy(data, playerId, serviceId);
        if (policy != null) {
            location.setPolicy(policy);
        }
        return location;
    }

    private Policy extractPolicy(C4VersionData data, Long playerId, Long serviceId) {
        String availability = data.getLookup().get(C4AtomApi.DC_TERMS_AVAILABLE);
        if (availability == null) {
            return null;
        }
        
        Matcher matcher = AVAILABILTY_RANGE_PATTERN.matcher(availability);
        checkArgument(matcher.matches(), "Unrecognized availability range format: %s", availability);
        
        String txDate = data.getLookup().get(C4AtomApi.DC_TX_DATE);
        Policy policy = null;
        policy = new Policy()
            .withAvailabilityStart(new DateTime(Strings.isNullOrEmpty(txDate) ? matcher.group(1) : txDate))
            .withAvailabilityEnd(new DateTime(matcher.group(2)))
            .withRevenueContract(RevenueContract.FREE_TO_VIEW)
            .withPlayer(playerId)
            .withService(serviceId);

        Set<Country> availableCountries = getAvailableCountries(data);    
        if (availableCountries != null) {
            policy.setAvailableCountries(availableCountries);
        }
        
        if(platform.isPresent()) {
            policy.setPlatform(platform.get());
        }
        policy.setLastUpdated(data.getLastUpdated());
        return policy;
    }

    private Set<Country> getAvailableCountries(C4VersionData data) {
        Element mediaGroup = data.getMediaGroup();
        Set<Country> availableCountries = null;
        if (mediaGroup != null) {
            Element restriction = mediaGroup.getChild("restriction", C4AtomApi.NS_MEDIA_RSS);
            if (restriction != null && restriction.getValue() != null) {
                availableCountries = Countries.fromDelimtedList(restriction.getValue());
            }
        }
        return availableCountries;
    }

    private Location embedLocation(String embedId, Location linkLocation) {
        Location location = new Location();
        location.setTransportType(TransportType.EMBED);
        location.setTransportSubType(TransportSubType.BRIGHTCOVE);
        location.setLastUpdated(linkLocation.getLastUpdated());
        location.setEmbedId(embedId);
        location.setEmbedCode(EMBED_CODE.replaceAll(VIDEOID_PLACEHOLDER, embedId));
        
        // The feed only contains available content
        location.setAvailable(linkLocation.getAvailable());
        if (linkLocation.getPolicy() != null) {
            location.setPolicy(linkLocation.getPolicy().copy());
        }

        return location;
    }
}
