package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Content;
import org.jdom.Element;
import org.jdom.Namespace;
import org.joda.time.Duration;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Maps;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;
import com.sun.syndication.feed.atom.Link;

public class C4AtomApi {
	
	private static final Splitter DURATION_SPLITTER = Splitter.on(":");

    public static final Namespace NS_MEDIA_RSS = Namespace.getNamespace("http://search.yahoo.com/mrss/");

    private static final String C4_WEB_ROOT = "http://www.channel4.com/";
    public static final String WEB_BASE = C4_WEB_ROOT + "programmes/";
    private static final String C4_PMLSC_ROOT = "http://pmlsc.channel4.com/";
    public static final String PROGRAMMES_BASE = C4_PMLSC_ROOT + "pmlsd/";

	public static final String WEB_SAFE_NAME_PATTERN = "[a-z0-9\\-]+";
	
	private static final Pattern CANONICAL_BRAND_URI_PATTERN = Pattern.compile(String.format("%s(%s)", Pattern.quote(PROGRAMMES_BASE), WEB_SAFE_NAME_PATTERN));

	private static final Pattern ATLAS_CANONICAL_ID_PATTERN = Pattern.compile("^http://.*\\/pmlsd\\/(.*)");
	
	private static final String FEED_ID_PREFIX_PATTERN = "tag:[a-z0-9.]+\\.channel4\\.com,\\d{4}:/programmes/";
    private static final Pattern BRAND_PAGE_ID_PATTERN = Pattern.compile(String.format("%s(%s)", FEED_ID_PREFIX_PATTERN, WEB_SAFE_NAME_PATTERN));
    private static final Pattern SERIES_PAGE_ID_PATTERN = Pattern.compile(String.format("%s(%s/episode-guide/series-\\d+)", FEED_ID_PREFIX_PATTERN, WEB_SAFE_NAME_PATTERN));
    private static final Pattern EPISODE_PAGE_LINK_ID_PATTERN = Pattern.compile("^.*\\/pmlsd/([^\\/]+/episode-guide/series-\\d+/episode-\\d+).atom.*");

	private static final String FEED_ID_CANONICAL_PREFIX = "tag:pmlsc.channel4.com,2009:/programmes/";
	
	public static final String DC_EPISODE_NUMBER = "dc:relation.EpisodeNumber";
	public static final String DC_SERIES_NUMBER = "dc:relation.SeriesNumber";
    public static final String DC_PROGRAMME_ID = "dc:relation.programmeId";
	public static final String DC_TERMS_AVAILABLE = "dcterms:available";
	public static final String DC_TX_DATE = "dc:date.TXDate";
	public static final String DC_DURATION = "dc:relation.Duration";

	private static final Pattern IMAGE_PATTERN = Pattern.compile("https?://.+\\.channel4\\.com/(.+?)\\d+x\\d+(\\.[a-zA-Z]+)");
	private static final Pattern BIPS_IMAGE_PATTERN = Pattern.compile("https?://.+\\.channel4.com\\/bips/(\\d+x\\d+)/videos/.*");
	private static final String IMAGE_SIZE = "625x352";
	private static final String THUMBNAIL_SIZE = "200x113";
	private static final String IOS_URI_PREFIX = "c4-4od://ios.channel4.com/pmlsd/";
	private static final Pattern WEB_4OD_BRAND_ID_EXTRACTOR = Pattern.compile(String.format("^%s(.+?)/4od\\#\\d+", WEB_BASE));

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
	
	public static void addImages(Content content, String anImage) {
		if (!Strings.isNullOrEmpty(anImage)) {
			Matcher matcher = IMAGE_PATTERN.matcher(anImage);
			if (matcher.matches()) {
				content.setThumbnail(C4_WEB_ROOT + matcher.group(1) + THUMBNAIL_SIZE + matcher.group(2));
				content.setImage((C4_WEB_ROOT + matcher.group(1) + IMAGE_SIZE + matcher.group(2)));
			} else {
			    Matcher bipsMatcher = BIPS_IMAGE_PATTERN.matcher(anImage);
			    if (bipsMatcher.matches()) {
			        String originalSize = bipsMatcher.group(1);
			        content.setThumbnail(anImage.replace(originalSize, THUMBNAIL_SIZE));
			        content.setImage(anImage.replace(originalSize, IMAGE_SIZE));
			    }
			}
		}
	}
	
	public static boolean isACanonicalBrandUri(String brandUri) {
		return CANONICAL_BRAND_URI_PATTERN.matcher(brandUri).matches();
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
        int seconds = 0;
        for (String part : DURATION_SPLITTER.split(durationString)) {
            seconds = (seconds * 60) + Integer.valueOf(part);
        }
        return Duration.standardSeconds(seconds);
    }

	public static boolean isABrandFeed(Feed source) {
		return BRAND_PAGE_ID_PATTERN.matcher(source.getId()).matches();
	}
	
	public static String canonicalizeBrandFeedId(Feed source) {
	    Matcher matcher = BRAND_PAGE_ID_PATTERN.matcher(source.getId());
	    if (matcher.matches()) {
	        return FEED_ID_CANONICAL_PREFIX + matcher.group(1);
	    }
	    return null;
	}

	public static boolean isASeriesFeed(Feed source) {
		return SERIES_PAGE_ID_PATTERN.matcher(source.getId()).matches();
	}

    public static String canonicalizeSeriesFeedId(Feed source) {
        Matcher matcher = SERIES_PAGE_ID_PATTERN.matcher(source.getId());
        if (matcher.matches()) {
            return FEED_ID_CANONICAL_PREFIX + matcher.group(1);
        }
        return null;
    }
    
    @SuppressWarnings("unchecked")
    public static String canonicalizeEpisodeFeedId(Entry source) {
        for (Link link : ((List<Link>) source.getOtherLinks())) {
            Matcher matcher = EPISODE_PAGE_LINK_ID_PATTERN.matcher(link.getHref());
            if (matcher.matches()) {
                return PROGRAMMES_BASE + matcher.group(1);
            }
        }
        return null;
    }

	public static String canonicalSeriesUri(Feed source) {
	    Matcher matcher = SERIES_PAGE_ID_PATTERN.matcher(source.getId());
        if (matcher.matches()) {
            return PROGRAMMES_BASE + matcher.group(1);
        }
        return null;
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

	public BiMap<String, Channel> getChannelMap() {
		return channelMap;
	}

    public static String hierarchyUriFromCanonical(String canonicalUri) {
        Matcher matcher = ATLAS_CANONICAL_ID_PATTERN.matcher(canonicalUri);
        if (matcher.matches()) {
            return PROGRAMMES_BASE + matcher.group(1);
        }
        return null;
    }

    public static String iOsUriFromPcUri(String uri) {
        Matcher matcher = WEB_4OD_BRAND_ID_EXTRACTOR.matcher(uri);
                
        if (matcher.matches()) {
            return IOS_URI_PREFIX + matcher.group(1) + "/4od.atom";
         }
        return null;
    }
	
}
