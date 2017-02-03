package org.atlasapi.output.simple;

import java.util.Set;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.output.Annotation;

public interface ModelSimplifier<F, T> {

    T simplify(F model, Set<Annotation> annotations, Application application);
    
}
