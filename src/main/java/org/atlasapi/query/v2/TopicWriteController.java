package org.atlasapi.query.v2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigInteger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;

import org.atlasapi.application.query.ApiKeyNotFoundException;
import org.atlasapi.application.query.ApplicationConfigurationFetcher;
import org.atlasapi.application.query.InvalidIpForApiKeyException;
import org.atlasapi.application.query.RevokedApiKeyException;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ModelTransformer;
import org.atlasapi.input.ReadException;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.simple.Description;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.topic.TopicStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.properties.Configurer;

public class TopicWriteController {

    private static final Logger log = LoggerFactory.getLogger(TopicWriteController.class);

    private final ApplicationConfigurationFetcher appConfigFetcher;
    private final TopicStore store;
    private final ModelReader reader;
    private static final NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private ModelTransformer<org.atlasapi.media.entity.simple.Topic, Topic> transformer;

    public TopicWriteController(ApplicationConfigurationFetcher appConfigFetcher, TopicStore store, ModelReader reader, ModelTransformer<org.atlasapi.media.entity.simple.Topic, Topic> transformer) {
        this.appConfigFetcher = appConfigFetcher;
        this.store = store;
        this.reader = reader;
        this.transformer = transformer;
    }
    
    
    @RequestMapping(value="/3.0/topics.json", method = RequestMethod.POST)
    public Void writeContent(HttpServletRequest req, HttpServletResponse resp) {
        
        Maybe<ApplicationConfiguration> possibleConfig;
        try {
            possibleConfig = appConfigFetcher.configurationFor(req);
        } catch (ApiKeyNotFoundException | RevokedApiKeyException | InvalidIpForApiKeyException e1) {
            return error(resp, HttpStatusCode.FORBIDDEN.code());
        }
        
        if (possibleConfig.isNothing()) {
            return error(resp, HttpStatus.UNAUTHORIZED.value());
        }
        
        Topic topic;
        try {
            topic = complexify(deserialize(new InputStreamReader(req.getInputStream())));
        } catch (IOException ioe) {
            log.error("Error reading input for request " + req.getRequestURL(), ioe);
            return error(resp, HttpStatusCode.SERVER_ERROR.code());
        } catch (Exception e) {
            log.error("Error reading input for request " + req.getRequestURL(), e);
            return error(resp, HttpStatusCode.BAD_REQUEST.code());
        }
        
        if (!possibleConfig.requireValue().canWrite(topic.getPublisher())) {
            return error(resp, HttpStatusCode.FORBIDDEN.code());
        }
        
        if (Strings.isNullOrEmpty(topic.getNamespace()) || Strings.isNullOrEmpty(topic.getValue())) {
            return error(resp, HttpStatusCode.BAD_REQUEST.code());
        }
        
        try {
            topic = merge(resolveExisting(topic), topic);
            store.write(topic);
        } catch (Exception e) {
            log.error("Error reading input for request " + req.getRequestURL(), e);
            return error(resp, HttpStatusCode.SERVER_ERROR.code());
        }

        String hostName = Configurer.get("local.host.name").get();
        resp.setHeader(
                HttpHeaders.LOCATION,
                hostName
                        + "/3.0/topic.json?id="
                        + codec.encode(BigInteger.valueOf(topic.getId())));
        resp.setStatus(HttpStatusCode.OK.code());
        resp.setContentLength(0);
        return null;
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

    private org.atlasapi.media.entity.simple.Topic deserialize(Reader input) throws IOException, ReadException {
        return reader.read(new BufferedReader(input), org.atlasapi.media.entity.simple.Topic.class);
    }
    
    private Void error(HttpServletResponse response, int code) {
        response.setStatus(code);
        response.setContentLength(0);
        return null;
    }
    
}
