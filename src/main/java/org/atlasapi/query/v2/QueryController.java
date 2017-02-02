/* Copyright 2009 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.query.v2;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.query.ApplicationFetcher;
import org.atlasapi.application.query.InvalidApiKeyException;
import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.simple.response.WriteResponse;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.output.QueryResult;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;
import org.atlasapi.persistence.event.EventContentLister;
import org.atlasapi.persistence.logging.AdapterLog;

import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class QueryController extends BaseController<QueryResult<Identified, ? extends Identified>> {
	
	private static final AtlasErrorSummary UNSUPPORTED = new AtlasErrorSummary(
	        new UnsupportedOperationException())
            .withErrorCode("UNSUPPORTED_VERSION")
            .withMessage("The requested version is no longer supported by this instance")
            .withStatusCode(HttpStatusCode.BAD_REQUEST);

	private static final AtlasErrorSummary FORBIDDEN = new AtlasErrorSummary(
	        new NullPointerException())
            .withStatusCode(HttpStatusCode.FORBIDDEN)
            .withMessage("Your API key is not permitted to view content from this publisher");

	private final KnownTypeQueryExecutor executor;

    private final ContentWriteController contentWriteController;
    private final EventContentLister contentLister;
	
    public QueryController(KnownTypeQueryExecutor executor,
            ApplicationFetcher configFetcher,
            AdapterLog log,
            AtlasModelWriter<QueryResult<Identified, ? extends Identified>> outputter,
            ContentWriteController contentWriteController,
            EventContentLister contentLister) {
	    super(configFetcher, log, outputter, SubstitutionTableNumberCodec.lowerCaseOnly(),
                DefaultApplication.createDefault());
        this.executor = executor;
        this.contentWriteController = contentWriteController;
        this.contentLister = contentLister;
	}
    
    @RequestMapping("/")
    public String redirect() {
        return "redirect:http://docs.atlasapi.org";
    }
    
    @RequestMapping(value = {"/2.0/*.*"})
    public void onePointZero(
            HttpServletRequest request,
            HttpServletResponse response
    ) throws IOException {
        outputter.writeError(request, response, UNSUPPORTED);
    }
	
	@RequestMapping("/3.0/discover.*")
	public void discover(
	        HttpServletRequest request,
            HttpServletResponse response
    ) throws IOException {
	    outputter.writeError(request, response, UNSUPPORTED);
	}
	
	@RequestMapping(value="/3.0/content.*",method=RequestMethod.GET)
	public void content(
	        HttpServletRequest request,
            HttpServletResponse response
    ) throws IOException {
		try {
            ContentQuery filter;
            try {
                filter = buildQuery(request);
            } catch (InvalidApiKeyException ex) {
                errorViewFor(request, response, AtlasErrorSummary.forException(ex));
                return;
            }
			
			List<String> uris = getUriList(request);
			if(!uris.isEmpty()) {
			    modelAndViewFor(
                        request,
                        response,
                        QueryResult.of(StreamSupport.stream(Iterables.concat(
                                executor.executeUriQuery(uris, filter)
                                        .values()).spliterator(),
                                false)
                                .filter((Identified.class)::isInstance)
                                .collect(Collectors.toList())
                        ),
                        filter.getApplication()
                );
			    return;
			}
			
		    List<String> ids = getIdList(request);
		    if(!ids.isEmpty()) {
		        modelAndViewFor(
		                request,
                        response,
                        QueryResult.of(StreamSupport.stream(Iterables.concat(
                                executor.executeIdQuery(decode(ids), filter)
                                        .values()).spliterator(),
                                false)
                                .filter((Identified.class)::isInstance)
                                .collect(Collectors.toList())
                        ),
                        filter.getApplication()
                );
		        return;
		    }
		    
	        List<String> values = getAliasValueList(request);
	        if (!values.isEmpty()) {
	            String namespace = getAliasNamespace(request);
	            modelAndViewFor(
	                    request,
                        response,
                        QueryResult.of(StreamSupport.stream(Iterables.concat(
                                executor.executeAliasQuery(
                                        Optional.fromNullable(namespace),
                                        values,
                                        filter
                                ).values()).spliterator(),
                                false)
                        .filter((Identified.class)::isInstance)
                        .collect(Collectors.toList())
                        ),
                        filter.getApplication()
                );
	            return;
	        }
	        
	        String publisher = request.getParameter("publisher");
	        
	        if (publisher != null) {
	            // Only a single publisher is supported, since it's the requirement and 
	            // is the most efficient index to build
    	        List<Publisher> publishers = ImmutableList.of(
    	                Publisher.fromKey(publisher).requireValue()
                );
    	        
                for (Publisher pub : publishers) {
                    if (!filter.getApplication().getConfiguration().isReadEnabled(pub)) {
                        errorViewFor(request, response, FORBIDDEN);
                        return;
                    }
                }
                modelAndViewFor(
                        request,
                        response,
                        QueryResult.of(StreamSupport.stream(Iterables.concat(
                                executor.executePublisherQuery(publishers, filter)
                                        .values()).spliterator(),
                        false)
                                .filter((Identified.class)::isInstance)
                                .collect(Collectors.toList())
                        ),
                        filter.getApplication()
                );
                return;
	        }

            List<String> eventIds = getEventRefIds(request);

            if(!eventIds.isEmpty()) {
                modelAndViewFor(
                        request,
                        response,
                        QueryResult.of(StreamSupport.stream(iterable(
                                contentLister.contentForEvent(
                                        ImmutableList.copyOf(decode(eventIds)),
                                        filter))
                                        .spliterator(),
                                false)
                                .filter((Identified.class)::isInstance)
                                .collect(Collectors.toList())
                        ),
                        filter.getApplication()
                );
                return;
            }
	            
	        throw new IllegalArgumentException("Must specify content uri(s) or id(s) or alias(es)");
			
		} catch (Exception e) {
			errorViewFor(request, response, AtlasErrorSummary.forException(e));
		}
	}

    private Iterable<Long> decode(List<String> ids) {
        return Lists.transform(ids, input -> idCodec.decode(input).longValue());
    }

    private List<String> getUriList(HttpServletRequest request) {
        return split(request.getParameter("uri"));
    }

    private List<String> getAliasValueList(HttpServletRequest request) {
        return split(request.getParameter("aliases.value"));
    }

    private String getAliasNamespace(HttpServletRequest request) {
        return request.getParameter("aliases.namespace");
    }

    private List<String> getIdList(HttpServletRequest request) {
        return split(request.getParameter("id"));
    }

    private List<String> getEventRefIds(HttpServletRequest request) {
        return split(request.getParameter("event_ids"));
    }

    private ImmutableList<String> split(String parameter) {
        if(parameter == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(URI_SPLITTER.split(parameter));
    }

    private Iterable<Content> iterable(final Iterator<Content> iterator) {
        return () -> iterator;
    }
    
    @RequestMapping(value="/3.0/content.json", method = RequestMethod.POST)
    public WriteResponse postContent(HttpServletRequest req, HttpServletResponse resp) {
        return contentWriteController.postContent(req, resp);
    }

    @RequestMapping(value="/3.0/content.json", method = RequestMethod.PUT)
    public WriteResponse putContent(HttpServletRequest req, HttpServletResponse resp) {
        return contentWriteController.putContent(req, resp);
    }

    @Nullable
    @RequestMapping(value="/3.0/content.json", method = RequestMethod.DELETE)
    public WriteResponse deleteContent(HttpServletRequest req, HttpServletResponse resp) {
        return contentWriteController.unpublishContent(req, resp);
    }
}
