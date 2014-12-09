package org.atlasapi.output.simple;

import static com.google.api.client.util.Preconditions.checkNotNull;

import java.math.BigInteger;
import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.feeds.youview.tasks.Response;
import org.atlasapi.feeds.youview.tasks.Task;
import org.atlasapi.feeds.youview.tasks.simple.Payload;
import org.atlasapi.output.Annotation;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.ids.NumberToShortStringCodec;


public class TaskModelSimplifier implements ModelSimplifier<Task, org.atlasapi.feeds.youview.tasks.simple.Task> {
    
    private final ResponseModelSimplifier responseSimplifier;
    private final NumberToShortStringCodec idCodec;

    public TaskModelSimplifier(NumberToShortStringCodec idCodec, ResponseModelSimplifier responseSimplifier) {
        this.idCodec = checkNotNull(idCodec);
        this.responseSimplifier = checkNotNull(responseSimplifier);
    }

    @Override
    public org.atlasapi.feeds.youview.tasks.simple.Task simplify(Task model,
            Set<Annotation> annotations, ApplicationConfiguration config) {
        org.atlasapi.feeds.youview.tasks.simple.Task task = new org.atlasapi.feeds.youview.tasks.simple.Task();
        
        task.setId(idCodec.encode(BigInteger.valueOf(model.id())));
        task.setPublisher(model.publisher());
        task.setAction(model.action());
        if (model.uploadTime().isPresent()) {
            task.setUploadTime(model.uploadTime().get().toDate());
        }
        task.setRemoteId(model.remoteId().orNull());
        task.setElementType(model.elementType());
        task.setElementId(model.elementId());
        task.setContent(model.content());
        task.setStatus(model.status());
        
        if (annotations.contains(Annotation.REMOTE_RESPONSES)) {
            task.setRemoteResponses(simplifyResponses(model.remoteResponses(), annotations, config));
        }
        if (annotations.contains(Annotation.PAYLOAD)) {
            task.setPayload(simplifyPayload(model.payload()));
        }
        
        return task;
    }

    private Iterable<org.atlasapi.feeds.youview.tasks.simple.Response> simplifyResponses(
            Set<Response> remoteResponses, final Set<Annotation> annotations, final ApplicationConfiguration config) {
        return FluentIterable.from(remoteResponses)
                .transform(simplifyResponse(annotations, config))
                .toSortedList(Ordering.natural());
    }

    private Function<Response, org.atlasapi.feeds.youview.tasks.simple.Response> simplifyResponse(
            final Set<Annotation> annotations, final ApplicationConfiguration config) {
        
        return new Function<Response, org.atlasapi.feeds.youview.tasks.simple.Response>() {
            @Override
            public org.atlasapi.feeds.youview.tasks.simple.Response apply(Response input) {
                return responseSimplifier.simplify(input, annotations, config);
            }
        };
    }
    
    private Payload simplifyPayload(org.atlasapi.feeds.youview.tasks.Payload payload) {
        Payload simplePayload = new Payload();
        
        simplePayload.setCreated(payload.created().toDate());
        simplePayload.setPayload(payload.payload());
        
        return simplePayload;
    }
}
