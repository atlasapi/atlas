package org.atlasapi.output.simple;

import static com.google.api.client.util.Preconditions.checkNotNull;

import java.math.BigInteger;
import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.feeds.tasks.Destination;
import org.atlasapi.feeds.tasks.Response;
import org.atlasapi.feeds.tasks.Task;
import org.atlasapi.feeds.tasks.YouViewDestination;
import org.atlasapi.feeds.tasks.simple.Payload;
import org.atlasapi.output.Annotation;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.ids.NumberToShortStringCodec;


public class TaskModelSimplifier implements ModelSimplifier<Task, org.atlasapi.feeds.tasks.simple.Task> {
    
    private final ResponseModelSimplifier responseSimplifier;
    private final NumberToShortStringCodec idCodec;

    public TaskModelSimplifier(NumberToShortStringCodec idCodec, ResponseModelSimplifier responseSimplifier) {
        this.idCodec = checkNotNull(idCodec);
        this.responseSimplifier = checkNotNull(responseSimplifier);
    }

    @Override
    public org.atlasapi.feeds.tasks.simple.Task simplify(Task model,
            Set<Annotation> annotations, ApplicationConfiguration config) {
        org.atlasapi.feeds.tasks.simple.Task task = new org.atlasapi.feeds.tasks.simple.Task();
        
        task.setId(idCodec.encode(BigInteger.valueOf(model.id())));
        task.setPublisher(model.publisher());
        task.setAction(model.action());
        if (model.uploadTime().isPresent()) {
            task.setUploadTime(model.uploadTime().get().toDate());
        }
        task.setRemoteId(model.remoteId().orNull());
        simplifyDestination(task, model.destination());
        task.setStatus(model.status());
        
        if (annotations.contains(Annotation.REMOTE_RESPONSES)) {
            task.setRemoteResponses(simplifyResponses(model.remoteResponses(), annotations, config));
        }
        if (annotations.contains(Annotation.PAYLOAD)) {
            task.setPayload(simplifyPayload(model.payload().orNull()));
        }
        
        return task;
    }

    private void simplifyDestination(org.atlasapi.feeds.tasks.simple.Task task,
            Destination destination) {
        task.setDestinationType(destination.type());
        switch(destination.type()) {
        case RADIOPLAYER:
            break;
        case YOUVIEW:
            YouViewDestination yVDest = (YouViewDestination) destination;
            task.setContentUri(yVDest.contentUri());        
            task.setElementType(yVDest.elementType());
            task.setElementId(yVDest.elementId());
            break;
        default:
            break;
        }
    }

    private Iterable<org.atlasapi.feeds.tasks.simple.Response> simplifyResponses(
            Set<Response> remoteResponses, final Set<Annotation> annotations, final ApplicationConfiguration config) {
        return FluentIterable.from(remoteResponses)
                .transform(simplifyResponse(annotations, config))
                .toSortedList(Ordering.natural());
    }

    private Function<Response, org.atlasapi.feeds.tasks.simple.Response> simplifyResponse(
            final Set<Annotation> annotations, final ApplicationConfiguration config) {
        
        return new Function<Response, org.atlasapi.feeds.tasks.simple.Response>() {
            @Override
            public org.atlasapi.feeds.tasks.simple.Response apply(Response input) {
                return responseSimplifier.simplify(input, annotations, config);
            }
        };
    }
    
    private Payload simplifyPayload(org.atlasapi.feeds.tasks.Payload payload) {
        if (payload == null) {
            return null;
        }
        
        Payload simplePayload = new Payload();
        
        simplePayload.setCreated(payload.created().toDate());
        simplePayload.setPayload(payload.payload());
        
        return simplePayload;
    }
}
