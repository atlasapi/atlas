package org.atlasapi.query.v2;

import java.io.IOException;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.query.ApiKeyNotFoundException;
import org.atlasapi.application.query.ApplicationConfigurationFetcher;
import org.atlasapi.application.query.InvalidIpForApiKeyException;
import org.atlasapi.application.query.RevokedApiKeyException;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.persistence.content.PeopleQueryResolver;
import org.atlasapi.persistence.logging.AdapterLog;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;

@Controller
public class PeopleController extends BaseController<Iterable<Person>> {

    private static final SelectionBuilder selectionBuilder = Selection.builder().withDefaultLimit(25).withMaxLimit(50);
    
    private static final AtlasErrorSummary NOT_FOUND = new AtlasErrorSummary(new NullPointerException())
        .withErrorCode("Person not found")
        .withStatusCode(HttpStatusCode.NOT_FOUND);
    private static final AtlasErrorSummary FORBIDDEN = new AtlasErrorSummary(new NullPointerException())
        .withStatusCode(HttpStatusCode.FORBIDDEN);

    private final PeopleQueryResolver resolver;
    private final PeopleWriteController personWriteController;

    public PeopleController(PeopleQueryResolver resolver, ApplicationConfigurationFetcher configFetcher,
                    AdapterLog log, AtlasModelWriter<Iterable<Person>> outputter, PeopleWriteController personWriteController) {
        super(configFetcher, log, outputter, SubstitutionTableNumberCodec.lowerCaseOnly());
        this.resolver = resolver;
        this.personWriteController = personWriteController;
    }

    @RequestMapping("/3.0/people.*")
    public void content(HttpServletRequest request, HttpServletResponse response) throws IOException {
        try {
            ApplicationConfiguration config;
            try {
                config = possibleAppConfig(request).valueOrDefault(ApplicationConfiguration
                        .defaultConfiguration());
            } catch (ApiKeyNotFoundException | RevokedApiKeyException | InvalidIpForApiKeyException ex) {
                outputter.writeError(request, response, FORBIDDEN);
                return;
            }

            String uri = request.getParameter("uri");
            String id = request.getParameter("id");
            String publisher = request.getParameter("publisher");
            if (Strings.isNullOrEmpty(uri) ^ Strings.isNullOrEmpty(id) ^ Strings.isNullOrEmpty(publisher) ) {
                throw new IllegalArgumentException("specify exactly one of 'uri', 'id' or 'publisher'");
            }
            
            Iterable<Person> people;
            if (uri != null || id != null) {
                Optional<Person> person;
                if (uri != null) {
                    person = resolver.person(uri, config);
                } else {
                    person = resolver.person(idCodec.decode(id).longValue(), config);
                }
                if(!person.isPresent()) {
                    errorViewFor(request, response, NOT_FOUND);
                    return;
                }
                people = person.asSet();
            } else {
                List<Publisher> publishers = Publisher.fromCsv(publisher);
                for (Publisher pub : publishers) {
                    if (!config.isEnabled(pub)) {
                        errorViewFor(request, response, FORBIDDEN);
                        return;
                    }
                }
                people = resolver.people(publishers, config, selectionBuilder.build(request));
            }
            
            if(Iterables.size(people) == 0) {
                errorViewFor(request, response, NOT_FOUND);
                return;
            }         
            
            modelAndViewFor(request, response, people, config);
        } catch (Exception e) {
            errorViewFor(request, response, AtlasErrorSummary.forException(e));
        }
    }
    
    @RequestMapping(value="/3.0/people.json", method = RequestMethod.POST)
    public Void postContent(HttpServletRequest req, HttpServletResponse resp) {
        return personWriteController.postPerson(req, resp);
    }

    @RequestMapping(value="/3.0/people.json", method = RequestMethod.PUT)
    public Void putContent(HttpServletRequest req, HttpServletResponse resp) {
        return personWriteController.putPerson(req, resp);
    }

}
