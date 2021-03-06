    package org.atlasapi.query.v2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigInteger;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.ConstraintViolationException;
import javax.ws.rs.core.HttpHeaders;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.application.query.ApplicationFetcher;
import org.atlasapi.application.query.ApplicationNotFoundException;
import org.atlasapi.application.query.InvalidApiKeyException;
import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ModelTransformer;
import org.atlasapi.input.ReadException;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.simple.response.WriteResponse;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.output.exceptions.ForbiddenException;
import org.atlasapi.persistence.topic.TopicStore;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.properties.Configurer;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

public class TopicWriteController {

    private static final Logger log = LoggerFactory.getLogger(TopicWriteController.class);

    private final ApplicationFetcher appConfigFetcher;
    private final TopicStore store;
    private final ModelReader reader;
    private static final NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private ModelTransformer<org.atlasapi.media.entity.simple.Topic, Topic> transformer;
    private AtlasModelWriter<Iterable<Topic>> outputter;
    private static final String STRICT = "strict";

    public TopicWriteController(ApplicationFetcher appConfigFetcher, TopicStore store,
            ModelReader reader,
            ModelTransformer<org.atlasapi.media.entity.simple.Topic, Topic> transformer,
            AtlasModelWriter<Iterable<Topic>> outputter) {
        this.appConfigFetcher = appConfigFetcher;
        this.store = store;
        this.reader = reader;
        this.transformer = transformer;
        this.outputter = outputter;
    }


    @RequestMapping(value="/3.0/topics.json", method = RequestMethod.POST)
    public WriteResponse writeContent(HttpServletRequest req, HttpServletResponse resp) {
        Boolean strict = Boolean.valueOf(req.getParameter(STRICT));

        Optional<Application> possibleApp;
        try {
            possibleApp = appConfigFetcher.applicationFor(req);
        } catch (InvalidApiKeyException e) {
            return error(req, resp, AtlasErrorSummary.forException(e));

        }

        if (!possibleApp.isPresent()) {
            log.error("No application found for request: {}", req.getRequestURL().toString());
            return error(
                    req,
                    resp,
                    AtlasErrorSummary.forException(
                            ApplicationNotFoundException.create(req.getRequestURL().toString())
                    )
            );
        }

        Application application = possibleApp.get();

        Topic topic;
        try {
            topic = complexify(deserialize(new InputStreamReader(req.getInputStream()), strict));

        } catch (UnrecognizedPropertyException |
                JsonParseException |
                ConstraintViolationException e) {
            return error(req, resp, AtlasErrorSummary.forException(e));

        } catch (IOException ioe) {
            log.error("Error reading input for request " + req.getRequestURL(), ioe);
            return error(req, resp, AtlasErrorSummary.forException(ioe));

        } catch (Exception e) {
            log.error("Error reading input for request " + req.getRequestURL(), e);
            AtlasErrorSummary errorSummary = new AtlasErrorSummary(e)
                    .withMessage("Error reading input for the request")
                    .withStatusCode(HttpStatusCode.BAD_REQUEST);
            return error(req, resp, errorSummary);

        }

        if (!application.getConfiguration().isWriteEnabled(topic.getPublisher())) {
            return error(
                    req,
                    resp,
                    AtlasErrorSummary.forException(new ForbiddenException(
                            "API key does not have write permission"))
            );
        }

        if (Strings.isNullOrEmpty(topic.getNamespace())
                || Strings.isNullOrEmpty(topic.getValue())) {
            AtlasErrorSummary errorSummary = new AtlasErrorSummary()
                    .withMessage("namespace or value must not be null or empty")
                    .withStatusCode(HttpStatusCode.BAD_REQUEST);
            return error(req, resp, errorSummary);
        }

        try {
            topic = merge(resolveExisting(topic), topic);
            store.write(topic);
        } catch (Exception e) {
            log.error("Error reading input for request " + req.getRequestURL(), e);
            AtlasErrorSummary errorSummary = new AtlasErrorSummary(e)
                    .withStatusCode(HttpStatusCode.BAD_REQUEST);
            return error(req, resp, errorSummary);
        }

        String hostName = Configurer.get("local.host.name").get();
        resp.setHeader(
                HttpHeaders.LOCATION,
                hostName
                        + "/3.0/topic.json?id="
                        + codec.encode(BigInteger.valueOf(topic.getId()))
        );
        resp.setStatus(HttpStatusCode.OK.code());

        return new WriteResponse(codec.encode(BigInteger.valueOf(topic.getId())));
    }

    private Topic merge(Maybe<Topic> possibleExisting, Topic posted) {
        if (possibleExisting.isNothing()) {
            return posted;
        }
        return merge(possibleExisting.requireValue(), posted);
    }

    private Topic merge(Topic existing, Topic posted) {
        existing.setType(posted.getType());
        existing.setTitle(posted.getTitle());
        existing.setDescription(posted.getDescription());
        existing.setImage(posted.getImage());
        existing.setThumbnail(posted.getThumbnail());
        return existing;
    }

    private Maybe<Topic> resolveExisting(Topic topic) {
        return store.topicFor(topic.getPublisher(), topic.getNamespace(), topic.getValue());
    }

    private Topic complexify(org.atlasapi.media.entity.simple.Topic inputContent) {
        return transformer.transform(inputContent);
    }

    private org.atlasapi.media.entity.simple.Topic deserialize(Reader input, Boolean strict)
            throws IOException, ReadException {
        return reader.read(new BufferedReader(input), org.atlasapi.media.entity.simple.Topic.class, strict);
    }


    private WriteResponse error(HttpServletRequest request, HttpServletResponse response,
            AtlasErrorSummary summary) {
        try {
            outputter.writeError(request, response, summary);
        } catch (IOException e) {
        }
        return null;
    }

}
