///* Copyright 2009 British Broadcasting Corporation
//   Copyright 2009 Meta Broadcast Ltd
//
//Licensed under the Apache License, Version 2.0 (the "License"); you
//may not use this file except in compliance with the License. You may
//obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
//implied. See the License for the specific language governing
//permissions and limitations under the License. */
//
//package org.atlasapi.remotesite.synd;
//
//import java.util.List;
//import java.util.Set;
//import java.util.concurrent.TimeUnit;
//
//import org.apache.commons.lang.StringUtils;
//import org.atlasapi.media.entity.Container;
//import org.atlasapi.media.entity.Content;
//import org.atlasapi.media.entity.ContentGroup;
//import org.atlasapi.media.entity.Encoding;
//import org.atlasapi.media.entity.Identified;
//import org.atlasapi.media.entity.Item;
//import org.atlasapi.media.entity.Location;
//import org.atlasapi.media.entity.Policy;
//import org.atlasapi.media.entity.Version;
//import org.atlasapi.persistence.system.Fetcher;
//import org.atlasapi.remotesite.ContentExtractor;
//import org.atlasapi.remotesite.FetchException;
//import org.atlasapi.remotesite.bbc.BbcPodcastGenreMap;
//import org.joda.time.Duration;
//
//import com.google.common.collect.ImmutableList;
//import com.google.common.collect.Lists;
//import com.google.common.collect.Sets;
//import com.metabroadcast.common.intl.Countries;
//import com.sun.syndication.feed.opml.Outline;
//
///**
// * @author Robert Chatley (robert@metabroadcast.com)
// */
//public class OpmlGraphExtractor implements ContentExtractor<OpmlSource, ContentGroup> {
//
//	private final Fetcher<Identified> delegateFetcher;
//	private final BbcPodcastGenreMap genreMap = new BbcPodcastGenreMap();
//
//	public OpmlGraphExtractor(Fetcher<Identified> delegateFetcher) {
//		this.delegateFetcher = delegateFetcher;
//	}
//
//	public ContentGroup extract(OpmlSource source) {
//		ContentGroup playlist = outerPlaylist(source);
//
//		List<Outline> outlines = typedList(source.getFeed().getOutlines());
//	
//		playlist.setContents(fetchFeedsFor(outlines));
//		
//		return playlist;
//	}
//
//	private ContentGroup outerPlaylist(OpmlSource source) {
//		ContentGroup playlist = new ContentGroup();
//		playlist.setCanonicalUri(source.getUri());
//		playlist.setTitle(source.getTitle());
//		playlist.setDescription(source.getTitle());
//		return playlist;
//	}
//	
//	private List<Content> fetchFeedsFor(List<Outline> outlines) {
//		List<Content> playlists = Lists.newArrayList();
//		if (outlines != null) {
//			for (Outline outline : outlines) {
//				playlists.addAll(fetchFeedsFor(outline));
//			}
//		}
//		return playlists;
//	}
//
//	private List<Content> fetchFeedsFor(Outline outline) {
//		List<Content> playlists = Lists.newArrayList();
//
//		List<Outline> children = typedList(outline.getChildren());
//		playlists.addAll(fetchFeedsFor(children));
//		
//		if (outline.getXmlUrl() != null) {
//			playlists.addAll(fetchFeed(outline.getXmlUrl(), outline));
//		} else if (outline.getHtmlUrl() != null) {
//			playlists.addAll(fetchFeed(outline.getHtmlUrl(), outline));
//		} else if (outline.getUrl() != null) {
//			playlists.addAll(fetchFeed(outline.getUrl(), outline));
//		}
//		return playlists;
//	}
//
//	@SuppressWarnings("unchecked")
//	private List<Container<Item>> fetchFeed(String feedUrl, Outline outline) {
//		
//		String genres = outline.getAttributeValue("bbcgenres");
//		
//		Container<Item> feed;
//		
//		try {
//			 feed = (Container<Item>) delegateFetcher.fetch(feedUrl);
//		} catch (FetchException fe) {
//			// carry on and try the next feed
//			return ImmutableList.of();
//		} 
//		
//		if (!StringUtils.isEmpty(genres)) {
//			Set<String> allGenres = Sets.newHashSet(feed.getGenres());
//			allGenres.addAll(map(genres));
//			feed.setGenres(allGenres);
//		}
//		
//		addPublishedDurationFor(outline, feed);
//		
//		addRestrictionsFor(outline, feed);
//		
//		return ImmutableList.of(feed);
//	}
//
//	private void addRestrictionsFor(Outline outline, Container<Item> feed) {
//		
//		String allow = outline.getAttributeValue("allow");
//		
//		for (Location location : locationsFrom(feed.getContents())) {
//			if (location.getUri() != null && location.getUri().contains("bbc.co.uk")) {
//				Policy policy = location.getPolicy();
//				if (policy == null) {
//					policy = new Policy();
//					location.setPolicy(policy);
//				}
//				policy.setAvailabilityLength((int) TimeUnit.SECONDS.convert(7, TimeUnit.DAYS));
//				if ("uk".equals(allow)) {
//					policy.addAvailableCountry(Countries.GB);
//				} 
//			}
//		}
//	}
//
//	private Iterable<Location> locationsFrom(Iterable<Item> items) {
//		Set<Location> locations = Sets.newHashSet();
//		for (Item item : items) {
//			for (Version version : item.getVersions()) {
//				for (Encoding encoding : version.getManifestedAs()) {
//					for (Location location : encoding.getAvailableAt()) {
//						locations.add(location);
//					}
//				}
//			}
//		}
//		return locations;
//	}
//
//	private void addPublishedDurationFor(Outline outline, Container<Item> feed) {
//
//		// foreach version 'publishedDuration' = typicalDurationMins x 60 for secs
//
//		String typicalDurationAttr = outline.getAttributeValue("typicalDurationMins");
//	
//		if (typicalDurationAttr != null) {
//			Integer typicalDuration = Integer.parseInt(typicalDurationAttr);
//			for (Item item : feed.getContents()) {
//				for (Version version : item.getVersions()) {
//					version.setDuration(Duration.standardMinutes(typicalDuration));
//				}
//			}
//		}
//	}
//
//	private Set<String> map(String pipeSeparatedGenreString) {
//
//		Set<String> results = Sets.newHashSet();
//		for (String genre : pipeSeparatedGenreString.split("\\|")) {
//			String mappedGenre = genreMap.lookup(genre);
//			if (mappedGenre != null) {
//				results.add(mappedGenre);
//			}
//		}
//		
//		return results;
//	}
//
//	@SuppressWarnings("unchecked")
//	private <T> List<T> typedList(List<?> list) {
//		return (List<T>)list;
//	}
//}
