package org.atlasapi.application.users;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ReadException;
import org.atlasapi.media.common.Id;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.common.QueryParser;
import org.atlasapi.query.common.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.common.base.Optional;
import com.metabroadcast.common.ids.NumberToShortStringCodec;

@Controller
public class UsersController {
    private static Logger log = LoggerFactory.getLogger(UsersController.class);
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final QueryParser<User> requestParser;
    private final QueryExecutor<User> queryExecutor;
    private final QueryResultWriter<User> resultWriter;
    private final ModelReader reader;
    private final NumberToShortStringCodec idCodec;
    private final UserFetcher userFetcher;
    private final UserStore userStore;
    
    public UsersController(QueryParser<User> requestParser,
            QueryExecutor<User> queryExecutor, 
            QueryResultWriter<User> resultWriter,
            ModelReader reader,
            NumberToShortStringCodec idCodec,
            UserFetcher userFetcher,
            UserStore userStore) {
        this.requestParser = requestParser;
        this.queryExecutor = queryExecutor;
        this.resultWriter = resultWriter;
        this.reader = reader;
        this.idCodec = idCodec;
        this.userFetcher = userFetcher;
        this.userStore = userStore;
    }

    @RequestMapping({ "/4.0/users/{uid}.*", "/4.0/users.*" })
    public void outputUsers(HttpServletRequest request, HttpServletResponse response) throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            Query<User> applicationsQuery = requestParser.parse(request);
            QueryResult<User> queryResult = queryExecutor.execute(applicationsQuery);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }
    
    @RequestMapping(value = "/4.0/users/{uid}.*", method = RequestMethod.POST)
    public void updateUser(HttpServletRequest request, 
            HttpServletResponse response,
            @PathVariable String uid) throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            Id userId = Id.valueOf(idCodec.decode(uid));
            Optional<User> user = userStore.userForId(userId);
            if (user.isPresent()) {
                User posted = deserialize(new InputStreamReader(request.getInputStream()), User.class);
                User modified = updateProfileFields(posted, user.get());
                userStore.store(modified);
                QueryResult<User> queryResult = QueryResult.singleResult(modified, QueryContext.standard());
                resultWriter.write(queryResult, writer);
            } else {
                throw new NotFoundException(userId);
            }
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }
    
    /**
     * Only allow certain fields to be updated
     */
    private User updateProfileFields(User posted, User user) {
        return user.copy()
                .withFullName(posted.getFullName())
                .withCompany(posted.getCompany())
                .withEmail(posted.getEmail())
                .withWebsite(posted.getWebsite())
                .withProfileComplete(posted.isProfileComplete())
                .build();
                
    }
    
    
    private <T> T deserialize(Reader input, Class<T> cls) throws IOException, ReadException {
        return reader.read(new BufferedReader(input), cls);
    }

}
