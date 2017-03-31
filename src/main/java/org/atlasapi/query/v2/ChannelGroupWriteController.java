package org.atlasapi.query.v2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
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
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.entity.simple.response.WriteResponse;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.output.exceptions.ForbiddenException;
import org.atlasapi.output.exceptions.UnauthorizedException;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.api.client.util.Lists;
import com.google.common.base.Strings;
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
    private static final AtlasErrorSummary BAD_REQUEST = AtlasErrorSummary.forException(new IllegalArgumentException(
            "The request sent couldn't be processed because it was syntactically incorrect."
    ));

    private final ModelReader reader;
    private final ChannelGroupStore store;
    private final ApplicationFetcher applicationFetcher;
    private final ChannelGroupResolver channelGroupResolver;
    private final AtlasModelWriter<Iterable<ChannelGroup>> outputWriter;
    private final ChannelGroupTransformer transformer;

    private SubstitutionTableNumberCodec idCodec = new SubstitutionTableNumberCodec();

    private ChannelGroupWriteController(Builder builder) {
        this.reader = checkNotNull(builder.reader);
        this.store = checkNotNull(builder.store);
        this.applicationFetcher = checkNotNull(builder.applicationFetcher);
        this.channelGroupResolver = checkNotNull(builder.channelGroupResolver);
        this.outputWriter = checkNotNull(builder.outputWriter);
        this.transformer = checkNotNull(builder.transformer);
    }

    public static Builder builder() {
        return new Builder();
    }

    public WriteResponse createPlatform(
            HttpServletRequest request,
            HttpServletResponse response,
            boolean createNewPlatform
    ) {
        return deserializeAndUpdateChannelGroup(request, response, createNewPlatform);
    }

    public WriteResponse updatePlatform(
            HttpServletRequest req,
            HttpServletResponse resp,
            boolean createNewPlatform
    ) {
        return deserializeAndUpdateChannelGroup(req, resp, createNewPlatform);
    }

    public WriteResponse deletePlatform(
            String id,
            HttpServletRequest request,
            HttpServletResponse response
    ) {
        return deletePlatformById(id, request, response);
    }

    private WriteResponse deserializeAndUpdateChannelGroup(
            HttpServletRequest request,
            HttpServletResponse response,
            boolean createNewPlatform
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
        ChannelGroup complexChannelGroup;
        org.atlasapi.media.entity.simple.ChannelGroup simpleChannelGroup;
        try {
            simpleChannelGroup = deserialize(
                    new InputStreamReader(request.getInputStream()),
                    strict
            );
            complexChannelGroup = complexify(simpleChannelGroup);
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
                .isWriteEnabled(complexChannelGroup.getPublisher())) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            return error(request, response, FORBIDDEN);
        }

        // write to DB
        try {
            long channelGroupId = store.createOrUpdate(complexChannelGroup).getId();
            if (createNewPlatform) {
                com.google.common.base.Optional<ChannelGroup> channelGroupToUpdate = store.channelGroupFor(
                        channelGroupId
                );
                if (channelGroupToUpdate.isPresent()) {
                    updateChannelGroupNumberings(channelGroupToUpdate.get(), simpleChannelGroup);
                    store.createOrUpdate(channelGroupToUpdate.get());
                } else {
                    log.error("Couldn't find a platform for requested ID {}", channelGroupId);
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    return error(request, response, BAD_REQUEST);
                }
            }
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

    private org.atlasapi.media.entity.simple.ChannelGroup deserialize(Reader input, Boolean strict)
            throws IOException, ReadException {
        return reader.read(
                new BufferedReader(input),
                org.atlasapi.media.entity.simple.ChannelGroup.class,
                strict
        );
    }

    private ChannelGroup complexify(
            org.atlasapi.media.entity.simple.ChannelGroup simpleChannelGroup
    ) {
        return transformer.transform(simpleChannelGroup);
    }

    private ChannelGroup updateChannelGroupNumberings(
            ChannelGroup channelGroupToBeUpdated,
            org.atlasapi.media.entity.simple.ChannelGroup simple
    ) {
        List<ChannelNumbering> channelNumberingList = Lists.newArrayList();

        simple.getChannels().forEach(channelNumbering -> channelNumberingList.add(
                ChannelNumbering.builder()
                        .withChannel(idCodec.decode(channelNumbering.getChannel().getId())
                                .longValue())
                        .withChannelGroup(channelGroupToBeUpdated.getId())
                        .build()
        ));
        channelGroupToBeUpdated.setChannelNumberings(channelNumberingList);

        return channelGroupToBeUpdated;
    }

    private WriteResponse deletePlatformById(
            String id,
            HttpServletRequest request,
            HttpServletResponse response
    ) {
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

        if (Strings.isNullOrEmpty(id)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return error(
                    request,
                    response,
                    AtlasErrorSummary.forException(new IllegalArgumentException(
                            "You must specify a platform ID for this action."
                    ))
            );
        }

        long channelGroupId = idCodec.decode(id).longValue();
        com.google.common.base.Optional<ChannelGroup> possibleChannelGroup = channelGroupResolver.channelGroupFor(
                channelGroupId
        );
        if (!possibleChannelGroup.isPresent()) {
            return error(request, response, AtlasErrorSummary.forException(new NullPointerException(
                    String.format("No such platform exists with ID %s", id)
            )));
        }

        if (!possibleApplication.get()
                .getConfiguration()
                .isWriteEnabled(possibleChannelGroup.get().getPublisher())) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            return error(request, response, FORBIDDEN);
        }

        try {
            store.deleteChannelGroupById(channelGroupId);
        } catch (Exception e) {
            log.error(
                    String.format(
                            "Error while deleting platform for request %s",
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

    public static class Builder {

        private ModelReader reader;
        private ChannelGroupStore store;
        private ApplicationFetcher applicationFetcher;
        private ChannelGroupResolver channelGroupResolver;
        private AtlasModelWriter<Iterable<ChannelGroup>> outputWriter;
        private ChannelGroupTransformer transformer;

        public Builder() {
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
            return new ChannelGroupWriteController(this);
        }
    }
}
