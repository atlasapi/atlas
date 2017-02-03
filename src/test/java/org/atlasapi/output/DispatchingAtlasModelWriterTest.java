package org.atlasapi.output;

import java.io.IOException;

import com.metabroadcast.applications.client.model.internal.Application;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.servlet.StubHttpServletRequest;
import com.metabroadcast.common.servlet.StubHttpServletResponse;

import static org.mockito.Mockito.mock;

public class DispatchingAtlasModelWriterTest {

    private final Mockery context = new Mockery();
    
    @SuppressWarnings("unchecked")
    private final AtlasModelWriter<String> delegate = context.mock(AtlasModelWriter.class);
    
    private final AtlasModelWriter<String> writer = DispatchingAtlasModelWriter.<String>dispatchingModelWriter()
                .register(delegate, "rdf.xml", MimeType.APPLICATION_RDF_XML).build();
    private Application application = mock(Application.class);
    
    @Test
    public void testSelectsWriterForExtension() throws IOException {
        
        final StubHttpServletRequest request = new StubHttpServletRequest().withRequestUri("/3.0/content.rdf.xml");
        final StubHttpServletResponse response = new StubHttpServletResponse();
        final String model = "Hello";

        context.checking(new Expectations(){{
            one(delegate).writeTo(request, response, model, ImmutableSet.of(), application);
        }});
        
        writer.writeTo(request, response, model, ImmutableSet.of(), application);
        
        context.assertIsSatisfied();
    }

}
