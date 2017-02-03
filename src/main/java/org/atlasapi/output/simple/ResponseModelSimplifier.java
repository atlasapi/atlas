package org.atlasapi.output.simple;

import java.util.Set;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.feeds.tasks.Response;
import org.atlasapi.output.Annotation;


public class ResponseModelSimplifier implements ModelSimplifier<Response, org.atlasapi.feeds.tasks.simple.Response> {

    @Override
    public org.atlasapi.feeds.tasks.simple.Response simplify(
            Response model,
            Set<Annotation> annotations,
            Application application
    ) {
        org.atlasapi.feeds.tasks.simple.Response response = new org.atlasapi.feeds.tasks.simple.Response();
        
        response.setStatus(model.status());
        response.setCreated(model.created().toDate());
        response.setPayload(model.payload());
        
        return response;
    }

}
