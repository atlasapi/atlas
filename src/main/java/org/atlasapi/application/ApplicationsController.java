package org.atlasapi.application;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ReadException;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.output.useraware.UserAwareQueryResultWriter;
import org.atlasapi.persistence.application.ApplicationStore;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.useraware.UserAwareQuery;
import org.atlasapi.query.common.useraware.UserAwareQueryContext;
import org.atlasapi.query.common.useraware.UserAwareQueryExecutor;
import org.atlasapi.query.common.useraware.UserAwareQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.ids.NumberToShortStringCodec;

@Controller
public class ApplicationsController {

    private static Logger log = LoggerFactory.getLogger(ApplicationsController.class);
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final UserAwareQueryParser<Application> requestParser;
    private final UserAwareQueryExecutor<Application> queryExecutor;
    private final UserAwareQueryResultWriter<Application> resultWriter;
    private final ModelReader reader;
    private final NumberToShortStringCodec idCodec;
    private final SourceIdCodec sourceIdCodec;
    private final ApplicationStore applicationStore;
    private final UserFetcher userFetcher;
    
    private static class PrecedenceOrdering {
        private List<String> ordering;
        public List<String> getOrdering() {
            return ordering;
        }
    }

    public ApplicationsController(UserAwareQueryParser<Application> requestParser,
            UserAwareQueryExecutor<Application> queryExecutor,
            UserAwareQueryResultWriter<Application> resultWriter,
            ModelReader reader,
            NumberToShortStringCodec idCodec,
            SourceIdCodec sourceIdCodec,
            ApplicationStore applicationStore,
            UserFetcher userFetcher) {
        this.requestParser = requestParser;
        this.queryExecutor = queryExecutor;
        this.resultWriter = resultWriter;
        this.reader = reader;
        this.idCodec = idCodec;
        this.sourceIdCodec = sourceIdCodec;
        this.applicationStore = applicationStore;
        this.userFetcher = userFetcher;
    }

    @RequestMapping({ "/4.0/applications/{aid}.*", "/4.0/applications.*" })
    public void outputAllApplications(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            UserAwareQuery<Application> applicationsQuery = requestParser.parse(request);
            UserAwareQueryResult<Application> queryResult = queryExecutor.execute(applicationsQuery);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    @RequestMapping(value = "/4.0/applications.*", method = RequestMethod.POST)
    public void writeApplication(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            Application application = deserialize(new InputStreamReader(request.getInputStream()), Application.class);
            if (application.getId() != null) {
                Optional<Application> existing = applicationStore.applicationFor(application.getId());
                application = application.copy().withSlug(existing.get().getSlug()).build();
                application = applicationStore.updateApplication(application);
            } else {
                // New application
                application = applicationStore.createApplication(application);
            }
            UserAwareQueryResult<Application> queryResult = UserAwareQueryResult.singleResult(application, UserAwareQueryContext.standard());

            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    @RequestMapping(value = "/4.0/applications/{aid}/sources", method = RequestMethod.POST)
    public void writeApplicationSources(HttpServletRequest request, 
            HttpServletResponse response,
            @PathVariable String aid)
            throws IOException {
        response.addHeader("Access-Control-Allow-Origin", "*");
        Id applicationId = Id.valueOf(idCodec.decode(aid));
        ApplicationSources sources;
        try {
            sources = deserialize(new InputStreamReader(
                    request.getInputStream()), ApplicationSources.class);
            Application existing = applicationStore.applicationFor(applicationId).get();
            Application modified = existing.copyWithSources(sources);
            applicationStore.updateApplication(modified);
        } catch (ReadException e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, null, request, response);
        }
    }
    
    @RequestMapping(value = "/4.0/applications/{aid}/precedence", method = RequestMethod.POST)
    public void setPrecedenceOrder(HttpServletRequest request, 
            HttpServletResponse response,
            @PathVariable String aid) throws IOException {
        response.addHeader("Access-Control-Allow-Origin", "*");
        Id applicationId = Id.valueOf(idCodec.decode(aid));
        PrecedenceOrdering ordering;
        try {
            ordering = deserialize(new InputStreamReader(request.getInputStream()), PrecedenceOrdering.class);
            List<Publisher> sourceOrder;
            sourceOrder = getSourcesFrom(ordering);
            Application existing = applicationStore.applicationFor(applicationId).get();
            applicationStore.updateApplication(existing.copyWithReadSourceOrder(sourceOrder));
        } catch (ReadException e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, null, request, response);
        } catch (QueryExecutionException e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, null, request, response);
        }
      
    }
    
    @RequestMapping(value = "/4.0/applications/{aid}/precedence", method = RequestMethod.DELETE)
    public void disablePrecedence(HttpServletRequest request, 
            HttpServletResponse response,
            @PathVariable String aid) {
        response.addHeader("Access-Control-Allow-Origin", "*");
        Id applicationId = Id.valueOf(idCodec.decode(aid));
        Application existing = applicationStore.applicationFor(applicationId).get();
        applicationStore.updateApplication(existing.copyWithPrecedenceDisabled());
    }

    private <T> T deserialize(Reader input, Class<T> cls) throws IOException, ReadException {
        return reader.read(new BufferedReader(input), cls);
    }
    
    private List<Publisher> getSourcesFrom(PrecedenceOrdering ordering) throws QueryExecutionException {
        ImmutableList.Builder<Publisher> sources =ImmutableList.builder();
        for (String sourceId : ordering.getOrdering()) {
            Optional<Publisher> source = sourceIdCodec.decode(sourceId);
            if (source.isPresent()) {
                sources.add(source.get());
            } else {
                throw new QueryExecutionException("No publisher by id " + sourceId);
            }
        }
        return sources.build();
    }
}
