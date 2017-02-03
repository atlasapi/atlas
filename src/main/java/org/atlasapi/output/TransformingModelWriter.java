package org.atlasapi.output;

import java.io.IOException;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.metabroadcast.applications.client.model.internal.Application;

public abstract class TransformingModelWriter<I, O> implements AtlasModelWriter<I> {

    private final AtlasModelWriter<O> delegate;

    public TransformingModelWriter(AtlasModelWriter<O> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void writeTo(
            HttpServletRequest request,
            HttpServletResponse response,
            I model,
            Set<Annotation> annotations,
            Application application
    ) throws IOException {
        delegate.writeTo(
                request,
                response,
                transform(model, annotations, application),
                annotations,
                application
        );
    }

    protected abstract O transform(
            I model,
            Set<Annotation> annotations,
            Application application
    );

    @Override
    public void writeError(
            HttpServletRequest request,
            HttpServletResponse response,
            AtlasErrorSummary exception
    ) throws IOException {
        delegate.writeError(request, response, exception);
    }

}
