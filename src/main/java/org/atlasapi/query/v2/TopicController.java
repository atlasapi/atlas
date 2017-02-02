package org.atlasapi.query.v2;

import java.io.IOException;
import java.util.Iterator;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.query.ApplicationFetcher;
import org.atlasapi.application.query.InvalidApiKeyException;
import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.simple.response.WriteResponse;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.output.QueryResult;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.topic.TopicContentLister;
import org.atlasapi.persistence.topic.TopicQueryResolver;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.query.Selection;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class TopicController extends BaseController<Iterable<Topic>> {

    private static final AtlasErrorSummary NOT_FOUND = new AtlasErrorSummary(new NullPointerException()).withErrorCode("TOPIC_NOT_FOUND").withStatusCode(HttpStatusCode.NOT_FOUND);
    private static final AtlasErrorSummary FORBIDDEN = new AtlasErrorSummary(new NullPointerException()).withErrorCode("TOPIC_UNAVAILABLE").withStatusCode(HttpStatusCode.FORBIDDEN);

    private final TopicQueryResolver topicResolver;
    private final TopicContentLister contentLister;
    private final QueryController queryController;
    private final TopicWriteController topicWriteController;

    public TopicController(TopicQueryResolver topicResolver, TopicContentLister contentLister, ApplicationFetcher configFetcher, AdapterLog log, AtlasModelWriter<Iterable<Topic>> atlasModelOutputter, QueryController queryController, TopicWriteController topicWriteController) {
        super(configFetcher, log, atlasModelOutputter, SubstitutionTableNumberCodec.lowerCaseOnly(),
                DefaultApplication.createDefault());
        this.topicResolver = topicResolver;
        this.contentLister = contentLister;
        this.queryController = queryController;
        this.topicWriteController = topicWriteController;
    }

    @RequestMapping(value={"3.0/topics.*","/topics.*"})
    public void topics(HttpServletRequest req, HttpServletResponse resp) throws IOException  {
        try {
            ContentQuery query;
            try {
                query = buildQuery(req);
            } catch (InvalidApiKeyException ex) {
                errorViewFor(req, resp, AtlasErrorSummary.forException(ex));
                return;
            }

            modelAndViewFor(req, resp, topicResolver.topicsFor(query), query.getApplication());
        } catch (Exception e) {
            errorViewFor(req, resp, AtlasErrorSummary.forException(e));
        }
    }
    
    @RequestMapping(value={"3.0/topics/{id}.*","/topics/{id}.*"})
    public void topic(HttpServletRequest req, HttpServletResponse resp, @PathVariable("id") String id) throws IOException {
        
        ContentQuery query;
        try {
            query = buildQuery(req);
        } catch (InvalidApiKeyException e) {
            outputter.writeError(req, resp, AtlasErrorSummary.forException(e));
            return;
        }
        
        Maybe<Topic> topicForUri = topicResolver.topicForId(idCodec.decode(id).longValue());
        
        if(topicForUri.isNothing()) {
            outputter.writeError(req, resp, NOT_FOUND.withMessage("Topic " + id + " not found"));
            return;
        }
        
        Topic topic = topicForUri.requireValue();
        
        if(!query.allowsSource(topic.getPublisher())) {
            outputter.writeError(req, resp, FORBIDDEN.withMessage("Topic " + id + " unavailable"));
            return;
        }
        
        
        modelAndViewFor(req, resp, ImmutableSet.of(topicForUri.requireValue()), query.getApplication());
    }
    
    @RequestMapping(value={"3.0/topics/{id}/content.*", "/topics/{id}/content"})
    public void topicContents(HttpServletRequest req, HttpServletResponse resp, @PathVariable("id") String id) throws IOException {
        ContentQuery query;
        try {
            query = buildQuery(req);
        } catch (InvalidApiKeyException e) {
            outputter.writeError(req, resp, AtlasErrorSummary.forException(e));
            return;
        }

        long decodedId = idCodec.decode(id).longValue();
        Maybe<Topic> topicForUri = topicResolver.topicForId(decodedId);
        
        if(topicForUri.isNothing()) {
            outputter.writeError(req, resp, NOT_FOUND.withMessage("Topic " + id + " not found"));
            return;
        }
        
        Topic topic = topicForUri.requireValue();
        
        if(!query.allowsSource(topic.getPublisher())) {
            outputter.writeError(req, resp, FORBIDDEN.withMessage("Topic " + id + " unavailable"));
            return;
        }
        
        try {
            Selection selection = query.getSelection();
            QueryResult<Identified, Topic> result = QueryResult.of(
                            Iterables.filter(iterable(contentLister.contentForTopic(decodedId, query)), Identified.class), 
                            topic);
            queryController.modelAndViewFor(req, resp, result.withSelection(selection), query.getApplication());
        } catch (Exception e) {
            errorViewFor(req, resp, AtlasErrorSummary.forException(e));
        }
    }

    private Iterable<Content> iterable(final Iterator<Content> iterator) {
        return () -> iterator;
    }
     
    @RequestMapping(value="/3.0/topics.json", method = RequestMethod.POST)
    public WriteResponse writeContent(HttpServletRequest req, HttpServletResponse resp) {
        return topicWriteController.writeContent(req, resp);
    }
}
