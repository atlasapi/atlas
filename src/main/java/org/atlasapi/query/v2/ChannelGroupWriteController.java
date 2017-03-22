package org.atlasapi.query.v2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.ConstraintViolationException;

import org.atlasapi.application.query.ApplicationFetcher;
import org.atlasapi.application.query.InvalidApiKeyException;
import org.atlasapi.input.ChannelGroupTransformer;
import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ReadException;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.entity.simple.response.WriteResponse;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.output.exceptions.ForbiddenException;
import org.atlasapi.output.exceptions.UnauthorizedException;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.http.HttpStatusCode;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupWriteController {

    private static final Logger log = LoggerFactory.getLogger(ChannelGroupWriteController.class);
    private static final String STRICT = "strict";

    private static final AtlasErrorSummary UNAUTHORIZED = AtlasErrorSummary.forException(new UnauthorizedException(
            "API key is unauthorised"
    ));
    private static final AtlasErrorSummary FORBIDDEN = AtlasErrorSummary.forException(new ForbiddenException(
            "API key does not have write permission"
    ));

    private final ModelReader reader;
    private final ChannelGroupStore store;
    private final ApplicationFetcher applicationFetcher;
    private final ChannelGroupResolver channelGroupResolver;
    private final AtlasModelWriter<Iterable<ChannelGroup>> outputWriter;
    private final ChannelGroupTransformer transformer;

    private ChannelGroupWriteController(
            ModelReader reader,
            ChannelGroupStore store,
            ApplicationFetcher applicationFetcher,
            ChannelGroupResolver channelGroupResolver,
            AtlasModelWriter<Iterable<ChannelGroup>> outputWriter,
            ChannelGroupTransformer transformer
    ) {
        this.reader = checkNotNull(reader);
        this.store = checkNotNull(store);
        this.applicationFetcher = checkNotNull(applicationFetcher);
        this.channelGroupResolver = checkNotNull(channelGroupResolver);
        this.outputWriter = checkNotNull(outputWriter);
        this.transformer = checkNotNull(transformer);
    }

    public static Builder builder() {
        return new Builder();
    }

    public WriteResponse createPlatform(HttpServletRequest request, HttpServletResponse response) {
        return deserializeAndUpdateChannelGroup(request, response);
    }

    private WriteResponse deserializeAndUpdateChannelGroup(
            HttpServletRequest request,
            HttpServletResponse response
    ) {
        Boolean strict = Boolean.valueOf(request.getParameter(STRICT));

        // authentication
        Optional<Application> possibleApplication;
        try {
            possibleApplication = applicationFetcher.applicationFor(request);
        } catch (InvalidApiKeyException ex) {
            return error(request, response, AtlasErrorSummary.forException(ex));
        }
        if (!possibleApplication.isPresent()) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return error(request, response, UNAUTHORIZED);
        }

        // deserialize JSON into a channelGroup & complexify it
        ChannelGroup channelGroup;
        try {
            channelGroup = complexify(
                    deserialize(
                            new InputStreamReader(request.getInputStream()),
                            strict
                    )
            );
        } catch (UnrecognizedPropertyException |
                JsonParseException |
                ConstraintViolationException e) {
            return error(request, response, AtlasErrorSummary.forException(e));

        } catch (IOException e) {
            log.error("Error reading input for request {}", request.getRequestURL(), e);
            return error(request, response, AtlasErrorSummary.forException(e));

        } catch (Exception e) {
            log.error("Error reading input for request {}", request.getRequestURL(), e);
            AtlasErrorSummary errorSummary = new AtlasErrorSummary(e)
                    .withStatusCode(HttpStatusCode.BAD_REQUEST);
            return error(request, response, errorSummary);
        }

        //authorization
        if (!possibleApplication.get()
                .getConfiguration()
                .isWriteEnabled(channelGroup.getPublisher())) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            return error(request, response, FORBIDDEN);
        }

        // write to DB
        try {
            store.createOrUpdate(channelGroup);
        } catch (Exception e) {
            log.error(
                    String.format(
                            "Error while creating/updating platform for request %s",
                            request.getRequestURL()
                    ),
                    e
            );
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return error(request, response, AtlasErrorSummary.forException(e));
        }

        response.setStatus(HttpServletResponse.SC_OK);

        return null;
    }

    //    public WriteResponse deletePlatform(
    //            String id,
    //            HttpServletRequest request,
    //            HttpServletResponse response
    //    ) {
    //        Optional<Application> possibleApplication;
    //        try {
    //            possibleApplication = applicationFetcher.applicationFor(request);
    //        } catch (InvalidApiKeyException ex) {
    //            return error(request, response, AtlasErrorSummary.forException(ex));
    //        }
    //        if (!possibleApplication.isPresent()) {
    //            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    //            return error(request, response, UNAUTHORIZED);
    //        }
    //
    //        if (Strings.isNullOrEmpty(id)) {
    //            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    //            return error(
    //                    request,
    //                    response,
    //                    AtlasErrorSummary.forException(new IllegalArgumentException(
    //                            "You must specify a platform ID for this action."
    //                    ))
    //            );
    //        }
    //
    //        Long channelGroupId = Long.valueOf(id);
    //        com.google.common.base.Optional<ChannelGroup> possibleChannelGroup = channelGroupResolver.channelGroupFor(
    //                channelGroupId);
    //        if (!possibleChannelGroup.isPresent()) {
    //            return error(request, response, AtlasErrorSummary.forException(new NullPointerException(
    //                    String.format("No such platform exists with ID %s", id)
    //            )));
    //        }
    //
    //        if (!possibleApplication.get()
    //                .getConfiguration()
    //                .isWriteEnabled(possibleChannelGroup.get().getPublisher())) {
    //            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
    //            return error(request, response, FORBIDDEN);
    //        }
    //
    //        try {
    //            store.deleteChannelGroupById(channelGroupId);
    //        } catch (Exception e) {
    //            log.error(
    //                    String.format(
    //                            "Error while deleting platform for request %s",
    //                            request.getRequestURL()
    //                    ),
    //                    e
    //            );
    //            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    //            return error(request, response, AtlasErrorSummary.forException(e));
    //        }
    //
    //        response.setStatus(HttpServletResponse.SC_OK);
    //
    //        return null;
    //    }

    private WriteResponse error(
            HttpServletRequest request,
            HttpServletResponse response,
            AtlasErrorSummary summary
    ) {
        try {
            outputWriter.writeError(request, response, summary);
        } catch (IOException e) {
            log.error("Error executing request {}", request.getRequestURL(), e);
        }
        return null;
    }

    private ChannelGroup complexify(
            org.atlasapi.media.entity.simple.ChannelGroup simpleChannelGroup
    ) {
        return transformer.transform(simpleChannelGroup);
    }

    private org.atlasapi.media.entity.simple.ChannelGroup deserialize(Reader input, Boolean strict)
            throws IOException, ReadException {
        return reader.read(
                new BufferedReader(input),
                org.atlasapi.media.entity.simple.ChannelGroup.class,
                strict
        );
    }

    public WriteResponse updatePlatform(HttpServletRequest req, HttpServletResponse resp) {
        return deserializeAndUpdateChannelGroup(req, resp);
    }

    public static class Builder {

        private ModelReader reader;
        private ChannelGroupStore store;
        private ApplicationFetcher applicationFetcher;
        private ChannelGroupResolver channelGroupResolver;
        private AtlasModelWriter<Iterable<ChannelGroup>> outputWriter;
        private ChannelGroupTransformer transformer;

        public Builder() {
            //
        }

        public Builder withReader(ModelReader reader) {
            this.reader = reader;
            return this;
        }

        public Builder withStore(ChannelGroupStore store) {
            this.store = store;
            return this;
        }

        public Builder withApplicationFetcher(ApplicationFetcher applicationFetcher) {
            this.applicationFetcher = applicationFetcher;
            return this;
        }

        public Builder withChannelGroupResolver(ChannelGroupResolver channelGroupResolver) {
            this.channelGroupResolver = channelGroupResolver;
            return this;
        }

        public Builder withOutputWriter(AtlasModelWriter<Iterable<ChannelGroup>> outputWriter) {
            this.outputWriter = outputWriter;
            return this;
        }

        public Builder withTransformer(ChannelGroupTransformer transformer) {
            this.transformer = transformer;
            return this;
        }

        public ChannelGroupWriteController build() {
            return new ChannelGroupWriteController(
                    reader,
                    store,
                    applicationFetcher,
                    channelGroupResolver,
                    outputWriter,
                    transformer
            );
        }
    }
}
