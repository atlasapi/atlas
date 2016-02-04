package org.atlasapi.remotesite.channel4;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metabroadcast.common.intl.Country;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;
import com.sun.syndication.feed.atom.Link;
import org.atlasapi.media.TransportType;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Policy.Platform;
import org.jdom.Element;
import org.jdom.Namespace;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class C4AtomApi {
	
	public static final Namespace NS_MEDIA_RSS = Namespace.getNamespace("http://search.yahoo.com/mrss/");

    public static final String DC_DURATION = "dc:relation.Duration";
	private static final String PROGRAMMES_BASE = "http://www.channel4.com/programmes/";
	
	private static final String WEB_SAFE_NAME_PATTERN = "[a-z0-9\\-]+";
	private static final String FEED_ID_PREFIX_PATTERN = "tag:www.channel4.com,\\d{4}:/programmes/";
	
	public static final Pattern SERIES_AND_EPISODE_NUMBER_IN_ANY_URI = Pattern.compile("series-(\\d+)/episode-(\\d+)");

	public static final Pattern BRAND_SERIES_AND_EPISODE_NUMBER_IN_ANY_PROGRAMMES_URI = Pattern.compile(String.format("/programmes/(%s)/episode-guide/series-(\\d+)/episode-(\\d+)", WEB_SAFE_NAME_PATTERN));
	
	public static final Pattern CANONICAL_BRAND_URI_PATTERN = Pattern.compile(String.format("%s(%s)", Pattern.quote(PROGRAMMES_BASE), WEB_SAFE_NAME_PATTERN));
	private static final Pattern CANONICAL_EPISODE_URI_PATTERN = Pattern.compile(String.format("%s%s/episode-guide/series-\\d+/episode-\\d+", Pattern.quote(PROGRAMMES_BASE), WEB_SAFE_NAME_PATTERN));

	private static final Pattern ANY_FEED_ID_PATTERN = Pattern.compile(String.format("%s(%s)/.*", FEED_ID_PREFIX_PATTERN, WEB_SAFE_NAME_PATTERN));

	private static final Pattern BRAND_PAGE_ID_PATTERN = Pattern.compile(String.format("%s%s", FEED_ID_PREFIX_PATTERN, WEB_SAFE_NAME_PATTERN));
	private static final Pattern SERIES_PAGE_ID_PATTERN = Pattern.compile(String.format("%s%s/episode-guide/series-\\d+", FEED_ID_PREFIX_PATTERN, WEB_SAFE_NAME_PATTERN));
	private static final Pattern EPISODE_PAGE_ID_PATTERN = Pattern.compile(String.format("%s%s/episode-guide/series-\\d+/episode-\\d+", FEED_ID_PREFIX_PATTERN, WEB_SAFE_NAME_PATTERN));

	private static final String API_BASE_URL = "http://api.channel4.com/pmlsd/";
	private static final String ATOZ_BASE_URL = "http://api.channel4.com/pmlsd/atoz/";
	
	public static final String DC_EPISODE_NUMBER = "dc:relation.EpisodeNumber";
	public static final String DC_SERIES_NUMBER = "dc:relation.SeriesNumber";
	public static final String DC_TERMS_AVAILABLE = "dcterms:available";
	public static final String DC_TX_DATE = "dc:date.TXDate";


	private static final Pattern IMAGE_PATTERN = Pattern.compile("(http.+?)\\d+x\\d+(\\.[a-zA-Z]+)");

	private static final String IMAGE_SIZE = "625x352";
	private static final String THUMBNAIL_SIZE = "200x113";
	
    public static final Pattern SLOT_PATTERN = Pattern.compile("tag:.*,\\d{4}:slot/(\\d+)");
	private static final Pattern AVAILABILTY_RANGE_PATTERN = Pattern.compile("start=(.*); end=(.*); scheme=W3C-DTF");

	private final BiMap<String, Channel> channelMap;
	
	

	public C4AtomApi(ChannelResolver channelResolver) {
		channelMap = ImmutableBiMap.<String, Channel>builder()
	            .put("C4", channelResolver.fromUri("http://www.channel4.com").requireValue())
	            .put("M4", channelResolver.fromUri("http://www.channel4.com/more4").requireValue())
	            .put("F4", channelResolver.fromUri("http://film4.com").requireValue())
	            .put("E4", channelResolver.fromUri("http://www.e4.com").requireValue())
	            .put("4M", channelResolver.fromUri("http://www.4music.com").requireValue())
	            .put("4S", channelResolver.fromUri("http://www.channel4.com/4seven").requireValue())
	            .build();
	}
	
	public static void addImages(Described content, String anImage) {
		if (! Strings.isNullOrEmpty(anImage)) {
			Matcher matcher = IMAGE_PATTERN.matcher(anImage);
			if (matcher.matches()) {
                String url = matcher.group(1) + IMAGE_SIZE + matcher.group(2);
				Image image = new Image(url);

                List<String> genericImageUrlList = createGenerateImageUrlList();

				if(genericImageUrlList.contains(url)){
					image.setType(ImageType.GENERIC_IMAGE_CONTENT_PLAYER);
				}
				content.setThumbnail(matcher.group(1) + THUMBNAIL_SIZE + matcher.group(2));
				content.setImages(ImmutableSet.of(image));
			}
		}

		if(content.getImages().size() > 0) {
			for(Image image : content.getImages()) {
				if(image.getType() != ImageType.GENERIC_IMAGE_CONTENT_PLAYER) {
					content.setImage(image.getCanonicalUri());
                    return;
				}
			}
		}
	}

	public static String canonicaliseEpisodeIdentifier(String uri) {
	    Matcher matcher = BRAND_SERIES_AND_EPISODE_NUMBER_IN_ANY_PROGRAMMES_URI.matcher(uri);
	    if (!matcher.find()) {
	        return null;
	    }
	    return String.format("%s%s/episode-guide/series-%s/episode-%s", PROGRAMMES_BASE, matcher.group(1), matcher.group(2), matcher.group(3));
	}
	
	public static boolean isACanonicalBrandUri(String brandUri) {
		return CANONICAL_BRAND_URI_PATTERN.matcher(brandUri).matches();
	}

	public static String requestForBrand(String brandCanonicalUri, String extension) {
		return createBrandRequest(extractWebSafeNameFromBrandUri(brandCanonicalUri), extension);
	}

	private static String extractWebSafeNameFromBrandUri(String brandCanonicalUri) {
		Matcher matcher = CANONICAL_BRAND_URI_PATTERN.matcher(brandCanonicalUri);
		Preconditions.checkArgument(matcher.matches(), "Not a valid brand URI: " + brandCanonicalUri);
		return matcher.group(1);
	}

	public static String createBrandRequest(String webSafeName, String extension) {
		return API_BASE_URL + webSafeName + extension;
	}
	
	public static String createAtoZRequest(String webSafeName, String extension) {
        return ATOZ_BASE_URL + webSafeName + extension;
    }

	public static boolean isACanonicalEpisodeUri(String href) {
		return CANONICAL_EPISODE_URI_PATTERN.matcher(href).matches();
	}

	public static String webSafeNameFromAnyFeedId(String id) {
		Matcher matcher = ANY_FEED_ID_PATTERN.matcher(id);
		if (matcher.matches()) {
			return matcher.group(1);
		}
		return null;
	}

	public static String episodeUri(String webSafeBrandName, int seriesNumber, int episodeNumber) {
		return seriesUriFor(webSafeBrandName, seriesNumber) + "/episode-" + episodeNumber;
	}

	public static String seriesUriFor(String webSafeBrandName, int seriesNumber) {
		return PROGRAMMES_BASE + webSafeBrandName + "/episode-guide/series-" + seriesNumber;
	}
	
	@SuppressWarnings("unchecked")
    public static String canonicalUri(Entry entry) {
        List<Link> links = entry.getAlternateLinks();
        
        for (Link link : links) {
            String href = link.getHref();
            if (C4AtomApi.isACanonicalEpisodeUri(href)) {
                return href;
            }
        }
        
        links = entry.getOtherLinks();
        for (Link link : links) {
            String href = link.getHref();
            if (C4AtomApi.isACanonicalEpisodeUri(href)) {
                return href;
            }
        }
        
        return null;
    }
	
	@SuppressWarnings("unchecked")
    public static Map<String, String> foreignElementLookup(Entry entry) {
        return foreignElementLookup((Iterable<Element>) entry.getForeignMarkup());
    }

    public static Map<String, String> foreignElementLookup(Iterable<Element> foreignMarkup) {
        Map<String, String> foreignElementLookup = Maps.newHashMap();
        for (Element element : foreignMarkup) {
            foreignElementLookup.put(element.getNamespacePrefix() + ":" + element.getName(), element.getText());
        }
        return foreignElementLookup;
    }
    
    public static Integer readAsNumber(Map<String, String> lookup, String key) {
        String value = lookup.get(key);
        if (Strings.isNullOrEmpty(value)) {
            return null;
        }
        return Integer.valueOf(value);
    }
    
    @SuppressWarnings("unchecked")
    public static String fourOdUri(Entry entry) {
        List<Link> links = entry.getAlternateLinks();
        
        for (Link link : links) {
            String href = link.getHref();
            if (href.contains("4od#")) {
                return href;
            }
        }
        return null;
    }
    
    public static Duration durationFrom(Map<String, String> lookup) {
        String durationString = lookup.get(DC_DURATION);
        if (durationString == null) {
            return null;
        }
        List<String> parts = Lists.newArrayList(Splitter.on(":").split(durationString));
        int duration = 0;
        for (String part : parts) {
            duration = (duration * 60) + Integer.valueOf(part);
        }
        return Duration.standardSeconds(duration);
    }

	public static boolean isABrandFeed(Feed source) {
		return BRAND_PAGE_ID_PATTERN.matcher(source.getId()).matches();
	}

	public static boolean isASeriesFeed(Feed source) {
		return SERIES_PAGE_ID_PATTERN.matcher(source.getId()).matches();
	}

	public static boolean isAnEpisodeId(String id) {
		return EPISODE_PAGE_ID_PATTERN.matcher(id).matches();
	}

	public static String clipUri(Entry entry) {
		 Element mediaGroup = mediaGroup(entry);
		 if (mediaGroup == null) {
			 return null;
		 }
		 Element player = mediaGroup.getChild("player", NS_MEDIA_RSS);
		 if (player == null) {
			 return null;
		 }
		 return player.getAttributeValue("url");
	}
	
	@SuppressWarnings("unchecked")
	public static Element mediaGroup(Entry syndEntry) {
		for (Element element : (List<Element>) syndEntry.getForeignMarkup()) {
			if (NS_MEDIA_RSS.equals(element.getNamespace()) && "group".equals(element.getName())) {
				return element;
			}
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public static Element mediaContent(Entry syndEntry) {
		for (Element element : (List<Element>) syndEntry.getForeignMarkup()) {
			if (NS_MEDIA_RSS.equals(element.getNamespace()) && "content".equals(element.getName())) {
				return element;
			}
		}
		return null;
	}
	
	public BiMap<String, Channel> getChannelMap() {
		return channelMap;
	}
	
	public static Location locationFrom(String uri, String locationId, Map<String, String> lookup, Set<Country> availableCountries, DateTime lastUpdated, Platform platform) {
		Location location = new Location();
		location.setUri(uri);
		
		if(locationId != null) {
		    // TODO new alias
			location.addAliasUrl(locationId);
		}
		location.setTransportType(TransportType.LINK);
		location.setLastUpdated(lastUpdated);
		
		// The feed only contains available content
		location.setAvailable(true);
		
		String availability = lookup.get(DC_TERMS_AVAILABLE);
		
		if (availability != null) {
			Matcher matcher = AVAILABILTY_RANGE_PATTERN.matcher(availability);
			if (!matcher.matches()) {
				throw new IllegalStateException("Availability range format not recognised, was " + availability);
			}
			String txDate = lookup.get(DC_TX_DATE);
            Policy policy = new Policy()
				.withAvailabilityStart(new DateTime(Strings.isNullOrEmpty(txDate) ? matcher.group(1) : txDate))
				.withAvailabilityEnd(new DateTime(matcher.group(2)));
				
			if (availableCountries != null) {
				policy.setAvailableCountries(availableCountries);
			}
			
			if(platform != null) {
				policy.setPlatform(platform);
			}
			
			location.setPolicy(policy);
		}
		return location;
	}

    private List<String> createGenerateImageUrlList() {
        //Update this if any generic images for C4 is found.
        return ImmutableList.of();
    }
}
