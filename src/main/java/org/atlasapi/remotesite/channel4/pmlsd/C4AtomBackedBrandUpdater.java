package org.atlasapi.remotesite.channel4.pmlsd;

import static com.google.common.base.Preconditions.checkArgument;
import static org.atlasapi.media.entity.Identified.TO_URI;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Policy.Platform;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.FetchException;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.sun.syndication.feed.atom.Feed;

public class C4AtomBackedBrandUpdater implements C4BrandUpdater {

    private static final Pattern BRAND_PAGE_PATTERN = Pattern.compile("http://pmlsc.channel4.com/pmlsd/([^/\\s]+)");

	private final Logger log = LoggerFactory.getLogger(getClass());
	
	private final C4AtomApiClient feedClient;
	private final C4AtomContentResolver resolver;
	private final ContentWriter writer;
	private final ContentExtractor<Feed, BrandSeriesAndEpisodes> extractor;
	private final Optional<Platform> platform;
	
	public C4AtomBackedBrandUpdater(C4AtomApiClient feedClient, Optional<Platform> platform, ContentResolver contentResolver, ContentWriter contentWriter, ContentExtractor<Feed, BrandSeriesAndEpisodes> extractor) {
		this.feedClient = feedClient;
        this.platform = platform;
		this.resolver = new C4AtomContentResolver(contentResolver);
		this.writer = contentWriter;
		this.extractor = extractor;
	}
	
	@Override
	public boolean canFetch(String uri) {
		return BRAND_PAGE_PATTERN.matcher(uri).matches();
	}

	public Brand createOrUpdateBrand(String uri) {
	    Preconditions.checkArgument(canFetch(uri), "Cannot fetch C4 uri: %s as it is not in the expected format: %s",uri, BRAND_PAGE_PATTERN.toString());

	    try {
			log.info("Fetching C4 brand " + uri);
			Optional<Feed> source = feedClient.brandFeed(uri);
			
			if (source.isPresent()) {
			    BrandSeriesAndEpisodes brandHierarchy = extractor.extract(source.get());
			    Brand brand = resolveAndUpdate(brandHierarchy.getBrand());
			    checkUri(brand);
			    checkSource(brand);
                writer.createOrUpdate(brand);
			    
			    write(brandHierarchy.getSeriesAndEpisodes(), brand);
			    
			    return brandHierarchy.getBrand();
			}
			throw new FetchException("Failed to fetch " + uri);
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

    private void write(SetMultimap<Series, Episode> seriesAndEpisodes, Brand brand) {
        for (Entry<Series, Collection<Episode>> seryAndEpisodes : seriesAndEpisodes.asMap().entrySet()) {
            Series series = null;
            if (seryAndEpisodes.getKey().getCanonicalUri() != null) {
                series = resolveAndUpdate(seryAndEpisodes.getKey());
                series.setParent(brand);
                checkUri(series);
                checkSource(brand);
                writer.createOrUpdate(series);
            }
            
            for (Episode episode : seryAndEpisodes.getValue()) {
                try {
                    episode = resolveAndUpdate(episode);
                    episode.setContainer(brand);
                    if (series != null) {
                        episode.setSeries(series);
                    }
                    checkUri(episode);
                    checkSource(episode);
                    writer.createOrUpdate(episode);
                } catch (Exception e) {
                    log.warn("Failed to write " + episode.getCanonicalUri(), e);
                }
            }
        }
    }
    
    private void checkUri(Content c) {
        String expectedPrefix = "http://" + C4PmlsdModule.PUBLISHER_TO_CANONICAL_URI_HOST_MAP.get(c.getPublisher()); 
        checkArgument(c.getCanonicalUri().startsWith(expectedPrefix),
            "%s doesn't start with %s", c, expectedPrefix);
    }

    private void checkSource(Content c) {
        checkArgument(c.getPublisher().equals(Publisher.C4_PMLSD)
                        || c.getPublisher().equals(Publisher.C4_PMLSD_P06),
                      "%s not %s", c, Publisher.C4_PMLSD);
    }

    private Episode resolveAndUpdate(Episode episode) {
        Optional<Item> existingEpisode = resolve(episode);
        if (!existingEpisode.isPresent()) {
            return episode;
        }
        return updateEpisode(ensureEpisode(existingEpisode.get()), episode);
    }
    
    private <E extends Episode> E updateEpisode(E existing, E fetched) {
        updateItem(existing, fetched);
        copyLastUpdated(fetched, existing);
        
        Integer episodeNumber = fetched.getEpisodeNumber();
        if (episodeNumber != null) {
            existing.setEpisodeNumber(episodeNumber);
        }
        
        Integer seriesNumber = fetched.getSeriesNumber();
        if (seriesNumber != null) {
            existing.setSeriesNumber(seriesNumber);
        }
        
        return existing;
    }
    

    private Episode ensureEpisode(Item item) {
        if (item instanceof Episode) {
            return (Episode) item;
        }
        return createAsEpisode(item);
    }

    private Episode createAsEpisode(Item item) {
        Episode episode = new Episode(item.getCanonicalUri(), item.getCurie(), item.getPublisher());
        episode.setAliases(item.getAliases());
        episode.setBlackAndWhite(item.getBlackAndWhite());
        episode.setClips(item.getClips());
        episode.setParentRef(item.getContainer());
        episode.setCountriesOfOrigin(item.getCountriesOfOrigin());
        episode.setDescription(item.getDescription());
        episode.setFirstSeen(item.getFirstSeen());
        episode.setGenres(item.getGenres());
        episode.setImage(item.getImage());
        episode.setIsLongForm(item.getIsLongForm());
        episode.setLastFetched(item.getLastFetched());
        episode.setLastUpdated(item.getLastUpdated());
        episode.setMediaType(item.getMediaType());
        episode.setPeople(item.getPeople());
        episode.setScheduleOnly(item.isScheduleOnly());
        episode.setSpecialization(item.getSpecialization());
        episode.setTags(item.getTags());
        episode.setThisOrChildLastUpdated(item.getThisOrChildLastUpdated());
        episode.setThumbnail(item.getThumbnail());
        episode.setTitle(item.getTitle());
        episode.setVersions(item.getVersions());
        return episode;
    }

    private Optional<Item> resolve(Episode episode) {
        return resolver.itemFor(episode.getCanonicalUri());
    }
    
    private Brand resolveAndUpdate(Brand brand) {
        Optional<Brand> existingBrand = resolver.brandFor(brand.getCanonicalUri());
        if (!existingBrand.isPresent()) {
            return brand;
        }
        return updateContent(existingBrand.get(), brand);
    }
    
    private Series resolveAndUpdate(Series series) {
        Optional<Series> existingSeries = resolver.seriesFor(series.getCanonicalUri());
        if (!existingSeries.isPresent()) {
            return series;
        }
        return updateContent(existingSeries.get(), series);
    }

    private <T extends Content> T updateContent(T existing, T fetched) {
        existing = updateDescribed(existing, fetched);
        
        Set<Clip> mergedClips = mergeClips(existing, fetched);
        existing.setClips(mergedClips);
    
        if (!Objects.equal(mergedClips, existing.getClips())) {
            copyLastUpdated(fetched, existing);
        }

        return existing;
    }

    private <T extends Content> Set<Clip> mergeClips(T existing, T fetched) {
        Set<Clip> mergedClips = Sets.newHashSet();
        ImmutableMap<String, Clip> fetchedClips = Maps.uniqueIndex(fetched.getClips(), TO_URI);
        for (Clip existingClip : existing.getClips()) {
            Clip fetchedClip = fetchedClips.get(existingClip.getCanonicalUri());
            if (fetchedClip != null) {
                mergedClips.add(updateItem(existingClip, fetchedClip));
            }
        }
        for (Clip fetchedClip : fetched.getClips()) {
            mergedClips.add(fetchedClip);
        }
        return mergedClips;
    }

    private <T extends Item> T updateItem(T existingClip, T fetchedClip) {
        existingClip = updateContent(existingClip, fetchedClip);
        Set<Version> versions = Sets.newHashSet();
        Version existingVersion = Iterables.getOnlyElement(existingClip.getVersions(), null);
        Version fetchedVersion = Iterables.getOnlyElement(fetchedClip.getVersions(), null);
        if(existingVersion != null || fetchedVersion != null) {
            versions.add(updateVersion(existingClip, existingVersion, fetchedVersion));
        }
        
        existingClip.setVersions(versions);
        return existingClip;
    }

    private Version updateVersion(Item item, Version existing, Version fetched) {
        if(existing == null) {
            return fetched;
        }
        if(fetched == null) {
            log.debug("Did not fetch a version for item {}", item.getCanonicalUri());
            return existing;
        }
        if (fetched.getDuration() != null && !Objects.equal(existing.getDuration(), fetched.getDuration())) {
            existing.setDuration(Duration.standardSeconds(fetched.getDuration()));
            copyLastUpdated(fetched, existing);
        }
        if (!equivalentRestrictions(existing.getRestriction(), fetched.getRestriction())) {
            existing.setRestriction(fetched.getRestriction());
            copyLastUpdated(fetched, existing);
        }

        Set<Broadcast> broadcasts = Sets.newHashSet();
        Map<String, Broadcast> fetchedBroadcasts = Maps.uniqueIndex(fetched.getBroadcasts(), new Function<Broadcast, String>() {
            @Override
            public String apply(Broadcast input) {
                return input.getSourceId();
            }
        });
        for (Broadcast broadcast : existing.getBroadcasts()) {
            Broadcast fetchedBroadcast = fetchedBroadcasts.get(broadcast.getSourceId());
            if (fetchedBroadcast != null) {
                broadcasts.add(updateBroadcast(broadcast, fetchedBroadcast));
            } else {
                broadcasts.add(broadcast);
            }
        }
        for (Broadcast broadcast : fetched.getBroadcasts()) {
            broadcasts.add(broadcast);
        }
        existing.setBroadcasts(broadcasts);
        
        Encoding existingEncoding = Iterables.getOnlyElement(existing.getManifestedAs(), null);
        Encoding fetchedEncoding = Iterables.getOnlyElement(fetched.getManifestedAs(), null);
        if(existingEncoding != null || fetchedEncoding != null) {
            existing.setManifestedAs(Sets.newHashSet(updateEncoding(existingEncoding, fetchedEncoding)));
        }
        else {
            existing.setManifestedAs(Sets.<Encoding>newHashSet());
        }
        return existing;
    }

    private Broadcast updateBroadcast(Broadcast existing, Broadcast fetched) {
        if (!Objects.equal(existing.getBroadcastOn(), fetched.getBroadcastOn())
            || !Objects.equal(existing.getTransmissionTime(), fetched.getTransmissionTime())
            || !Objects.equal(existing.getTransmissionEndTime(), fetched.getTransmissionEndTime())){
            fetched.setIsActivelyPublished(existing.isActivelyPublished());
            return fetched;
        }
        return existing;
    }

    private Encoding updateEncoding(Encoding existingEncoding, Encoding fetchedEncoding) {
        if(existingEncoding == null) {
            return fetchedEncoding;
        }
        if(fetchedEncoding == null) {
            return existingEncoding;
        }
        
        Set<Location> mergedLocations = Sets.newHashSet(findExistingLocationsForOtherPlatforms(existingEncoding.getAvailableAt()));
        for (Location fetchedLocation : fetchedEncoding.getAvailableAt()) {
            Location existingEquivalent = findExistingLocation(fetchedLocation, existingEncoding.getAvailableAt());
            if (existingEquivalent != null) {
                mergedLocations.add(updateLocation(existingEquivalent, fetchedLocation));
            } else {
                mergedLocations.add(fetchedLocation);
            }
        }
        
        existingEncoding.setAvailableAt(mergedLocations);
        
        return existingEncoding;
    }

    private Iterable<Location> findExistingLocationsForOtherPlatforms(
            Set<Location> availableAt) {
        return Iterables.filter(availableAt, new Predicate<Location>() {

            @Override
            public boolean apply(Location input) {
                if (platform.isPresent()) {
                    return input.getPolicy() == null || !platform.get().equals(input.getPolicy().getPlatform());
                } else {
                    return input.getPolicy() != null && input.getPolicy().getPlatform() != null;
                }
            }
            
        });
    }

    private Location updateLocation(Location existing, Location fetched) {
        if (!Objects.equal(existing.getAliases(), fetched.getAliases())) {
            existing.setAliases(fetched.getAliases());
            copyLastUpdated(fetched, existing);
        }
        if (!Objects.equal(existing.getEmbedCode(), fetched.getEmbedCode())) {
            existing.setEmbedCode(fetched.getEmbedCode());
            copyLastUpdated(fetched, existing);
        }
        if (!equivalentPolicy(existing.getPolicy(), fetched.getPolicy())) {
            existing.setPolicy(fetched.getPolicy());
            copyLastUpdated(fetched, existing);
        }
        return existing;
    }

    private Location findExistingLocation(Location fetched, Set<Location> existingLocations) {
        for (Location existing : existingLocations) {
            if (existing.getUri() != null && existing.getUri().equals(fetched.getUri())
                || existing.getEmbedId() != null && existing.getEmbedId().equals(fetched.getEmbedId())) {
                return existing;
            }
        }
        return null;
    }

    private boolean equivalentPolicy(Policy existing, Policy fetched) {
        return existing != null
            && fetched != null
            && Objects.equal(existing.getAvailabilityStart(), fetched.getAvailabilityStart())
            && Objects.equal(existing.getAvailabilityEnd(), fetched.getAvailabilityEnd())
            && Objects.equal(existing.getPlatform(), fetched.getPlatform())
            && Objects.equal(existing.getService(), fetched.getService())
            && Objects.equal(existing.getPlayer(), fetched.getPlayer())
            && Objects.equal(existing.getRevenueContract(), fetched.getRevenueContract())
            && Objects.equal(existing.getAvailableCountries(), fetched.getAvailableCountries());
    }
    
    private boolean equivalentRestrictions(Restriction existing, Restriction fetched) {
        return existing != null
            && fetched != null
            && Objects.equal(existing.isRestricted(), fetched.isRestricted())
            && Objects.equal(existing.getMessage(), fetched.getMessage())
            && Objects.equal(existing.getMinimumAge(), fetched.getMinimumAge());
    }

    private <T extends Described> T updateDescribed(T existing, T fetched) {
        
        SetView<String> mergedAliasUrls = Sets.union(existing.getAliasUrls(), fetched.getAliasUrls());
        
        if (!mergedAliasUrls.equals(existing.getAliasUrls())) {
            existing.setAliasUrls(mergedAliasUrls);
            copyLastUpdated(fetched, existing);
        }

        if (fetched.getAliases() != null) {
            existing.setAliases(fetched.getAliases());
            copyLastUpdated(fetched, existing);
        }
        
        return updateDescriptions(existing, fetched);
    }

    private <T extends Described> T updateDescriptions(T existing, T fetched) {
        if (!Objects.equal(existing.getTitle(), fetched.getTitle())) {
            existing.setTitle(fetched.getTitle());
            copyLastUpdated(fetched, existing);
        }
        if (!Objects.equal(existing.getDescription(), fetched.getDescription())) {
            existing.setDescription(fetched.getDescription());
            copyLastUpdated(fetched, existing);
        }
        if (!Objects.equal(existing.getImage(), fetched.getImage())) {
            existing.setImage(fetched.getImage());
            copyLastUpdated(fetched, existing);
        }
        if (!Objects.equal(existing.getThumbnail(), fetched.getThumbnail())) {
            existing.setThumbnail(fetched.getThumbnail());
            copyLastUpdated(fetched, existing);
        }
        if (!Objects.equal(existing.getGenres(), fetched.getGenres())) {
            existing.setGenres(fetched.getGenres());
            copyLastUpdated(fetched, existing);
        }
        if (!Objects.equal(existing.getPresentationChannel(), fetched.getPresentationChannel())) {
            existing.setPresentationChannel(fetched.getPresentationChannel());
            copyLastUpdated(fetched, existing);
        }
        return existing;
    }

    private void copyLastUpdated(Identified from, Identified to) {
        to.setLastUpdated(from.getLastUpdated());
    }

}
