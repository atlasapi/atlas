/* Copyright 2009 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.remotesite.itv;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.ContentExtractor;

import com.google.common.collect.Lists;


/**
 * {@link SiteSpecificRepresentationAdapter} for content from itv.com
 *  
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public class ItvGraphExtractor implements ContentExtractor<ItvBrandSource, List<Brand>> {

	private Brand brand(ItvProgramme sourceBrand) {
		String uri = sourceBrand.url();
		Brand brand = new Brand(uri, curieFrom(uri), Publisher.ITV);
		brand.setTitle(sourceBrand.title());
		return brand;
	}
	
	private Episode episode(ItvEpisode sourceEpisode, ItvProgramme sourceBrand) {
		String uri = sourceEpisode.url();
		Episode episode = new Episode(uri, curieFrom(uri), Publisher.ITV);
		episode.setTitle(sourceEpisode.date());
		episode.setDescription(sourceEpisode.description());
		episode.setThumbnail(sourceBrand.thumbnail());
		episode.setIsLongForm(true);
		return episode;
	}
	
	private static Pattern curiePattern = Pattern.compile(".*ViewType=(\\d+).*&Filter=(\\d+).*");
	
	private String curieFrom(String url) {
		Matcher matcher = curiePattern.matcher(url);
		if (matcher.find()) {
			return "itv:" + matcher.group(1) + "-" + matcher.group(2);
		}
		return null;
	}


	@Override
	public List<Brand> extract(ItvBrandSource source) {
	
		List<ItvProgramme> sourceBrands = source.brands();
		List<Brand> brands = Lists.newArrayList();
		
		for (ItvProgramme sourceBrand : sourceBrands) {
			
			Brand brand = brand(sourceBrand);
			brands.add(brand);
			
			for (ItvEpisode sourceEpisode : sourceBrand.episodes()) {
				
				Location location = location(sourceEpisode);
				
				Encoding encoding = new Encoding();
				encoding.addAvailableAt(location);

				Version version = new Version();
				version.addManifestedAs(encoding);
				
				Episode episode = episode(sourceEpisode, sourceBrand);
				episode.addVersion(version);
				
				brand.addItem(episode);
			}
		}
		
		return brands;
	}

	private Location location(ItvEpisode episode) {
		Location location = new Location();
		location.setUri(episode.url());
		location.setTransportType(TransportType.LINK);
		location.setAvailable(true);
		return location;
	}
}
