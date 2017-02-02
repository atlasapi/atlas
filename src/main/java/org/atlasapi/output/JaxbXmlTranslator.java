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

package org.atlasapi.output;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Serializer;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.simple.Broadcast;
import org.atlasapi.media.entity.simple.Channel;
import org.atlasapi.media.entity.simple.ChannelGroup;
import org.atlasapi.media.entity.simple.ChannelGroupQueryResult;
import org.atlasapi.media.entity.simple.ChannelQueryResult;
import org.atlasapi.media.entity.simple.ContentGroup;
import org.atlasapi.media.entity.simple.ContentGroupQueryResult;
import org.atlasapi.media.entity.simple.ContentQueryResult;
import org.atlasapi.media.entity.simple.Item;
import org.atlasapi.media.entity.simple.Location;
import org.atlasapi.media.entity.simple.PeopleQueryResult;
import org.atlasapi.media.entity.simple.Playlist;
import org.atlasapi.media.entity.simple.PublisherDetails;
import org.atlasapi.media.entity.simple.ScheduleQueryResult;
import org.atlasapi.media.entity.simple.Topic;
import org.atlasapi.media.entity.simple.TopicQueryResult;
import org.atlasapi.media.vocabulary.DC;
import org.atlasapi.media.vocabulary.PLAY_SIMPLE_XML;
import org.atlasapi.media.vocabulary.PO;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.metabroadcast.common.http.HttpHeaders;
import com.sun.xml.bind.marshaller.NamespacePrefixMapper;

/**
 * Outputs simple URIplay model in plain XML format using JAXB.
 * 
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public class JaxbXmlTranslator<T> implements AtlasModelWriter<T> {

    private static final String GZIP_HEADER_VALUE = "gzip";
    private static final String NS_MAPPER = "com.sun.xml.bind.namespacePrefixMapper";
	private static final UriplayNamespacePrefixMapper PREFIX_MAPPER = new UriplayNamespacePrefixMapper();
	
	private final JAXBContext context;

	public JaxbXmlTranslator(JAXBContext context) {
		this.context = context;
    }
	
	public JaxbXmlTranslator() {
	    this(standardContext());
    }

    public static JAXBContext standardContext() {
        try {
            return JAXBContext.newInstance(
                ContentQueryResult.class,
                ScheduleQueryResult.class,
                PeopleQueryResult.class,
                Playlist.class,
                Item.class,
                Location.class,
                Broadcast.class,
                PublisherDetails.class,
                ChannelGroup.class,
                Channel.class,
                ChannelQueryResult.class,
                ChannelGroupQueryResult.class,
                Topic.class, 
                TopicQueryResult.class,
                ContentGroup.class,
                ContentGroupQueryResult.class);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
	public void writeTo(final HttpServletRequest request, final HttpServletResponse response, T result, Set<Annotation> annotations, ApplicationConfiguration config) throws IOException {
		try {
            writeOut(request, response, result);
        } catch (JAXBException e) {
            writeError(request, response, AtlasErrorSummary.forException(e));
        }
	}

	private void writeOut(final HttpServletRequest request, final HttpServletResponse response, final Object result) throws JAXBException, IOException {
		Marshaller m = context.createMarshaller();
		m.setProperty(NS_MAPPER, PREFIX_MAPPER);
		OutputStream out = response.getOutputStream();
		
		String accepts = request.getHeader(HttpHeaders.ACCEPT_ENCODING);
        if (accepts != null && accepts.contains(GZIP_HEADER_VALUE)) {
            response.setHeader(HttpHeaders.CONTENT_ENCODING, GZIP_HEADER_VALUE);
            out = new GZIPOutputStream(out);
        }
        try {
            m.marshal(result, out);
        } finally {
		    if (out instanceof GZIPOutputStream) {
                ((GZIPOutputStream) out).finish();
            }
		}
	}
	
	private static final class UriplayNamespacePrefixMapper extends NamespacePrefixMapper {
		@Override
		public String getPreferredPrefix(String namespaceUri, String suggestion, boolean requirePrefix) {
			if (PLAY_SIMPLE_XML.NS.equals(namespaceUri)) {
				return PLAY_SIMPLE_XML.PREFIX;
			} else if (PO.NS.equals(namespaceUri)) {
				return PO.PREFIX;
			} else if (DC.NS.equals(namespaceUri)) {
				return "dc";
			}
			return null;
		}

		@Override
		public String[] getPreDeclaredNamespaceUris() {
		    return new String[] { PLAY_SIMPLE_XML.NS , PO.NS, DC.NS};
		}
	}
	
	@Override
	public void writeError(HttpServletRequest request, HttpServletResponse response, AtlasErrorSummary exception) {
		try {
			write(response.getOutputStream(), xmlFrom(exception));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private Element xmlFrom(AtlasErrorSummary exception) {
		Element error = new Element("error");
		error.appendChild(stringElement("message", exception.message()));
		error.appendChild(stringElement("code", exception.errorCode()));
		error.appendChild(stringElement("id", exception.id()));
		return error;
	}

	private void write(OutputStream out, Element xml) throws UnsupportedEncodingException, IOException {
		Serializer serializer = new Serializer(out, Charsets.UTF_8.toString());
		serializer.setIndent(4);
		serializer.setLineSeparator("\n");
		serializer.write(new Document(xml));
	}
	
	private Element stringElement(String name, String value) {
		Element elem = new Element(name);
		elem.appendChild(value);
		return elem;
	} 
}
