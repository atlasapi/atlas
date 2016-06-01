package org.atlasapi.output;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Serializer;

import org.atlasapi.application.v3.ApplicationConfiguration;

import com.google.common.base.Charsets;
import com.metabroadcast.common.http.HttpHeaders;

import tva.metadata._2010.TVAMainType;


public class JaxbTVAnytimeModelWriter implements AtlasModelWriter<JAXBElement<TVAMainType>> {
    
    private static final String GZIP_HEADER_VALUE = "gzip";

    @Override
    public void writeTo(HttpServletRequest request, HttpServletResponse response,
            JAXBElement<TVAMainType> result, Set<Annotation> annotations,
            ApplicationConfiguration config) throws IOException {
        try {
            writeOut(request, response, result);
        } catch (JAXBException e) {
            writeError(request, response, AtlasErrorSummary.forException(e));
        }
    }
    
    private void writeOut(final HttpServletRequest request, final HttpServletResponse response, 
            final JAXBElement<TVAMainType> result) throws JAXBException, IOException {
        
        JAXBContext context = JAXBContext.newInstance("tva.metadata._2010");
        Marshaller marshaller = context.createMarshaller();
        OutputStream out = response.getOutputStream();
        String accepts = request.getHeader(HttpHeaders.ACCEPT_ENCODING);
        if (accepts != null && accepts.contains(GZIP_HEADER_VALUE)) {
            response.setHeader(HttpHeaders.CONTENT_ENCODING, GZIP_HEADER_VALUE);
            out = new GZIPOutputStream(out);
        }
        try {
            marshaller.marshal(result, out);
        } finally {
            if (out instanceof GZIPOutputStream) {
                ((GZIPOutputStream) out).finish();
            }
        }
    }

    // this is identical to the JaxbXmlTranslator. It'd be good if this code didn't need to be duplicated...
    @Override
    public void writeError(HttpServletRequest request, HttpServletResponse response,
            AtlasErrorSummary exception) throws IOException {
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
