package org.atlasapi.application;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.output.useraware.UserAwareQueryResultWriter;
import org.atlasapi.query.common.useraware.UserAwareQuery;
import org.atlasapi.query.common.useraware.UserAwareQueryExecutor;
import org.atlasapi.query.common.useraware.UserAwareQueryParser;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.base.Optional;
import com.metabroadcast.common.ids.NumberToShortStringCodec;

@Controller
public class SourceRequestsController {
    private final UserAwareQueryParser<SourceRequest> queryParser;
    private final UserAwareQueryExecutor<SourceRequest> queryExecutor;
    private final UserAwareQueryResultWriter<SourceRequest> resultWriter;
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final SourceRequestManager sourceRequestManager;
    private final NumberToShortStringCodec idCodec;
    private final SourceIdCodec sourceIdCodec;
    private final UserFetcher userFetcher;
    
    public SourceRequestsController(UserAwareQueryParser<SourceRequest> queryParser,
            UserAwareQueryExecutor<SourceRequest> queryExecutor,
            UserAwareQueryResultWriter<SourceRequest> resultWriter,
            SourceRequestManager sourceRequestManager,
            NumberToShortStringCodec idCodec,
            SourceIdCodec sourceIdCodec,
            UserFetcher userFetcher) {
        this.queryParser = queryParser;
        this.queryExecutor = queryExecutor;
        this.resultWriter = resultWriter;
        this.sourceRequestManager = sourceRequestManager;
        this.idCodec = idCodec;
        this.sourceIdCodec = sourceIdCodec;
        this.userFetcher = userFetcher;
    }
    
    @RequestMapping(value = {"/4.0/requests.*", "/4.0/requests/{id}.*"}, method = RequestMethod.GET)
    public void listSourceRequests(HttpServletRequest request, 
            HttpServletResponse response) throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            UserAwareQuery<SourceRequest> sourcesQuery = queryParser.parse(request);
            UserAwareQueryResult<SourceRequest> queryResult = queryExecutor.execute(sourcesQuery);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
      
    }
  
    @RequestMapping(value = "/4.0/sources/{sid}/requests", method = RequestMethod.POST)
    public void storeSourceRequest(HttpServletRequest request, 
            HttpServletResponse response,
            @PathVariable String sid,
            @RequestParam String appId,
            @RequestParam String appUrl,
            @RequestParam String email,
            @RequestParam String reason,
            @RequestParam String usageType) throws IOException {

        response.addHeader("Access-Control-Allow-Origin", "*");
        try {
            Optional<Publisher> source =sourceIdCodec.decode(sid);
            if (source.isPresent()) {
                Id applicationId = Id.valueOf(idCodec.decode(appId));
                UsageType usageTypeRequested = UsageType.valueOf(usageType.toUpperCase());
                sourceRequestManager.createOrUpdateRequest(source.get(), usageTypeRequested,
                    applicationId, appUrl, email, reason);
            } else {
                throw new NotFoundException(null);
            }
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, null, request, response);
        }
    }
    
    @RequestMapping(value = "/4.0/requests/{rid}/approve", method = RequestMethod.POST)
    public void storeSourceRequest(HttpServletRequest request, 
            HttpServletResponse response,
            @PathVariable String rid) throws IOException {
        response.addHeader("Access-Control-Allow-Origin", "*");
        try {
            Id requestId = Id.valueOf(idCodec.decode(rid));
            sourceRequestManager.approveSourceRequest(requestId);
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, null, request, response);
        }
    }
}
