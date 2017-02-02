package org.atlasapi.output;

import java.io.IOException;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.servlet.StubHttpServletRequest;
import com.metabroadcast.common.servlet.StubHttpServletResponse;

public class DispatchingAtlasModelWriterTest {

    private final Mockery context = new Mockery();
    
    @SuppressWarnings("unchecked")
    private final AtlasModelWriter<String> delegate = context.mock(AtlasModelWriter.class);
    
    private final AtlasModelWriter<String> writer = DispatchingAtlasModelWriter.<String>dispatchingModelWriter()
                .register(delegate, "rdf.xml", MimeType.APPLICATION_RDF_XML).build();
    
    @Test
    public void testSelectsWriterForExtension() throws IOException {
        
        final StubHttpServletRequest request = new StubHttpServletRequest().withRequestUri("/3.0/content.rdf.xml");
        final StubHttpServletResponse response = new StubHttpServletResponse();
        final String model = "Hello";

        context.checking(new Expectations(){{
            one(delegate).writeTo(request, response, model, ImmutableSet.<Annotation>of(), ApplicationConfiguration.DEFAULT_CONFIGURATION);
        }});
        
        writer.writeTo(request, response, model, ImmutableSet.<Annotation>of(), ApplicationConfiguration.DEFAULT_CONFIGURATION);
        
        context.assertIsSatisfied();
    }

}
