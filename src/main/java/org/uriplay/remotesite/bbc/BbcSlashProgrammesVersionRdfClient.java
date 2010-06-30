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

package org.uriplay.remotesite.bbc;

import java.io.Reader;
import java.io.StringReader;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.uriplay.persistence.system.RemoteSiteClient;
import org.uriplay.remotesite.HttpClients;

import com.metabroadcast.common.http.SimpleHttpClient;

/**
 * Client to retrieve RDF XML representing a version, from BBC /programmes and 
 * bind it to our object model using JAXB. 
 *  
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public class BbcSlashProgrammesVersionRdfClient implements RemoteSiteClient<SlashProgrammesVersionRdf> {

	private final SimpleHttpClient httpClient;
	private final JAXBContext context;

	public BbcSlashProgrammesVersionRdfClient() {
		this(HttpClients.webserviceClient());
	}
	
	public BbcSlashProgrammesVersionRdfClient(SimpleHttpClient httpClient) {
		this.httpClient = httpClient;
		try {
			context = JAXBContext.newInstance(SlashProgrammesVersionRdf.class);
		} catch (JAXBException e) {
			throw new RuntimeException(e);
		}
	}

	public SlashProgrammesVersionRdf get(String uri) throws Exception {
		Reader in = new StringReader(httpClient.getContentsOf(uri));
		Unmarshaller u = context.createUnmarshaller();
		SlashProgrammesVersionRdf versionDescription = (SlashProgrammesVersionRdf) u.unmarshal(in);
		return versionDescription;
	}

}
