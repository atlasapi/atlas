/* Copyright 2009 British Broadcasting Corporation
   Copyright 2009 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.output.oembed;

import java.io.IOException;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Version;
import org.atlasapi.output.Annotation;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;

/**
 * General translator to build an oEmbed representation from information in the bean graph.
 *  
 * @author Robert Chatley (robert@metabroadcast.com)
 */
@Deprecated
public class OembedTranslator implements AtlasModelWriter<Iterable<Content>> {

	interface OutputFactory {
		OembedOutput createOutput();
	}
	
	private final OutputFactory feedFactory;
	
	public OembedTranslator(OutputFactory outputFactory) {
		this.feedFactory = outputFactory;
	}

	@Override
	public void writeTo(HttpServletRequest request, HttpServletResponse response, Iterable<Content> graph, Set<Annotation> annotations, ApplicationConfiguration config) throws IOException {

		OembedOutput output = feedFactory.createOutput();
		
		for (Object bean : graph) {
			
			if (bean instanceof Item) {
				
				Item item = (Item) bean;
				
				output.setTitle(item.getTitle());
				output.setProviderUrl(item.getPublisher().key());
				output.setType("video");
				
				if (item.getVersions() != null) {
					for (Version version : item.getVersions()) {
						if (version.getManifestedAs() != null) {
							for (Encoding encoding : version.getManifestedAs()) {
								if (encoding.getVideoVerticalSize() != null) {
									output.setHeight(encoding.getVideoVerticalSize());
								}
								if (encoding.getVideoHorizontalSize() != null) {
									output.setWidth(encoding.getVideoHorizontalSize());
								}
								for (Location location : encoding.getAvailableAt()) {
								    if (location.getEmbedCode() != null) {
								        output.setEmbedCode(escapeQuotes(location.getEmbedCode()));
								    }
								}
							}
						}
					}
				}
			}
		}
		
		output.writeTo(response.getOutputStream());
	}


	private String escapeQuotes(String unescaped) {
		if (unescaped == null) { return null; }
		return unescaped.replace("\"", "\\\"");
	}

	@Override
	public void writeError(HttpServletRequest request, HttpServletResponse response, AtlasErrorSummary exception) {
		//no-op
	}

}
