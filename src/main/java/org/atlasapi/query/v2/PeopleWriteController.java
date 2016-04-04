package org.atlasapi.query.v2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.query.ApiKeyNotFoundException;
import org.atlasapi.application.query.ApplicationConfigurationFetcher;
import org.atlasapi.application.query.InvalidIpForApiKeyException;
import org.atlasapi.application.query.RevokedApiKeyException;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ModelTransformer;
import org.atlasapi.input.ReadException;
import org.atlasapi.media.entity.Person;
import org.atlasapi.persistence.content.people.PersonStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;

public class PeopleWriteController {
    
    //TODO: replace with proper merge strategies.
    private static final boolean MERGE = true;
    private static final boolean OVERWRITE = false;

    private static final Logger log = LoggerFactory.getLogger(PeopleWriteController.class);

    private final ApplicationConfigurationFetcher appConfigFetcher;
    private final PersonStore store;
    private final ModelReader reader;

    private ModelTransformer<org.atlasapi.media.entity.simple.Person, Person> transformer;

    public PeopleWriteController(ApplicationConfigurationFetcher appConfigFetcher, PersonStore store, ModelReader reader, ModelTransformer<org.atlasapi.media.entity.simple.Person, Person> transformer) {
        this.appConfigFetcher = appConfigFetcher;
        this.store = store;
        this.reader = reader;
        this.transformer = transformer;
    }
    
    @RequestMapping(value="/3.0/person.json", method = RequestMethod.POST)
    public Void postPerson(HttpServletRequest req, HttpServletResponse resp) {
        return deserializeAndUpdatePerson(req, resp, MERGE);
    }

    @RequestMapping(value="/3.0/person.json", method = RequestMethod.PUT)
    public Void putPerson(HttpServletRequest req, HttpServletResponse resp) {
        return deserializeAndUpdatePerson(req, resp, OVERWRITE);
    }

    private Void deserializeAndUpdatePerson(HttpServletRequest req, HttpServletResponse resp,
            boolean merge) {
        Maybe<ApplicationConfiguration> possibleConfig;
        try {
            possibleConfig = appConfigFetcher.configurationFor(req);
        } catch (ApiKeyNotFoundException | RevokedApiKeyException | InvalidIpForApiKeyException e) {
            log.error("Problem with API key for request " + req.getRequestURL(), e);
            return error(resp, HttpStatus.UNAUTHORIZED.value());
        }
        
        if (possibleConfig.isNothing()) {
            return error(resp, HttpStatus.UNAUTHORIZED.value());
        }
        
        Person person;
        try {
            person = complexify(deserialize(new InputStreamReader(req.getInputStream())));
        } catch (IOException ioe) {
            log.error("Error reading input for request " + req.getRequestURL(), ioe);
            return error(resp, HttpStatusCode.SERVER_ERROR.code());
        } catch (Exception e) {
            log.error("Error reading input for request " + req.getRequestURL(), e);
            return error(resp, HttpStatusCode.BAD_REQUEST.code());
        }
        
        if (!possibleConfig.requireValue().canWrite(person.getPublisher())) {
            return error(resp, HttpStatusCode.FORBIDDEN.code());
        }
        
        if (Strings.isNullOrEmpty(person.getCanonicalUri())) {
            return error(resp, HttpStatusCode.BAD_REQUEST.code());
        }
        
        try {
            person = merge(resolveExisting(person), person, merge);
            store.createOrUpdatePerson(person);
        } catch (Exception e) {
            log.error("Error reading input for request " + req.getRequestURL(), e);
            return error(resp, HttpStatusCode.SERVER_ERROR.code());
        }
        
        resp.setStatus(HttpStatusCode.OK.code());
        resp.setContentLength(0);
        return null;
    }
    
    private Person merge(Optional<Person> possibleExisting, Person update, boolean merge) {
        if (!possibleExisting.isPresent()) {
            return update;
        }
        return merge(possibleExisting.get(), update, merge);
    }

    private Person merge(Person existing, Person update, boolean merge) {
        existing.setEquivalentTo(merge ? merge(existing.getEquivalentTo(), update.getEquivalentTo()) : update.getEquivalentTo());
        existing.setLastUpdated(update.getLastUpdated());
        existing.setTitle(update.getTitle());
        existing.setDescription(update.getDescription());
        existing.setImage(update.getImage());
        existing.setThumbnail(update.getThumbnail());
        existing.setMediaType(update.getMediaType());
        existing.setSpecialization(update.getSpecialization());
        existing.setRelatedLinks(merge ? merge(existing.getRelatedLinks(), update.getRelatedLinks()) : update.getRelatedLinks());
        existing.setGivenName(update.getGivenName());
        existing.setFamilyName(update.getFamilyName());
        existing.setGender(update.getGender());
        existing.setBirthDate(update.getBirthDate());
        existing.setBirthPlace(update.getBirthPlace());
        existing.setQuotes(merge ? merge(existing.getQuotes(),update.getQuotes()) : update.getQuotes());
        existing.setImages(merge ?
                           merge(existing.getImages(), update.getImages()) :
                           update.getImages());
        existing.setAwards(merge ?
                           merge(existing.getAwards(), update.getAwards()) :
                           update.getAwards());
        return existing;
    }

    private <T> Set<T> merge(Set<T> existing, Set<T> posted) {
        return ImmutableSet.copyOf(Iterables.concat(posted, existing));
    }

    private Optional<Person> resolveExisting(Person person) {
        return store.person(person.getCanonicalUri());
    }

    private Person complexify(org.atlasapi.media.entity.simple.Person inputPerson) {
        return transformer.transform(inputPerson);
    }

    private org.atlasapi.media.entity.simple.Person deserialize(Reader input) throws IOException, ReadException {
        return reader.read(new BufferedReader(input), org.atlasapi.media.entity.simple.Person.class);
    }
    
    private Void error(HttpServletResponse response, int code) {
        response.setStatus(code);
        response.setContentLength(0);
        return null;
    }
    
}
