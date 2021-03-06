package org.atlasapi.query.v2;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.application.query.ApplicationFetcher;
import org.atlasapi.application.query.InvalidApiKeyException;
import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ModelTransformer;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.Annotation;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.persistence.content.PeopleQueryResolver;
import org.atlasapi.persistence.content.people.PersonStore;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.NullAdapterLog;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.servlet.StubHttpServletRequest;
import com.metabroadcast.common.servlet.StubHttpServletResponse;


public class PeopleControllerTest {

    private PeopleQueryResolver resolver = mock(PeopleQueryResolver.class);
    private final ApplicationFetcher configFetcher = mock(ApplicationFetcher.class);
    private final AdapterLog log = new NullAdapterLog();
    @SuppressWarnings("unchecked")
    private final AtlasModelWriter<Iterable<Person>> outputter = mock(AtlasModelWriter.class);

    private Application application = mock(Application.class);
    private PersonStore store = mock(PersonStore.class);
    private ModelReader reader = mock(ModelReader.class);
    @SuppressWarnings("unchecked")
    private ModelTransformer<org.atlasapi.media.entity.simple.Person, Person> transformer = mock(ModelTransformer.class);
    private PeopleWriteController writeController = new PeopleWriteController(configFetcher, store, reader, transformer, outputter);

    private final PeopleController peopleController = new PeopleController(resolver, configFetcher, log, outputter, writeController, application);
    private final SubstitutionTableNumberCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private StubHttpServletRequest request;
    private StubHttpServletResponse response;

    @Before
    public void setup() throws InvalidApiKeyException {
        request = new StubHttpServletRequest();
        response = new StubHttpServletResponse();

        when(configFetcher.applicationFor(request))
            .thenReturn(java.util.Optional.empty());
    }
    
    @Test
    public void testFailsIfBothUriAndIdAreSpecified() throws Exception {
        request.withParam("uri", "uri");
        request.withParam("id", "id");
        
        peopleController.content(request, response);
        
        verify(outputter).writeError(argThat(is(request)), argThat(is(response)), any(AtlasErrorSummary.class));
        verify(outputter, never()).writeTo(argThat(is(request)), argThat(is(response)), anyPeople(), anySetOfPublishers(), any(Application.class));

    }
    
    @Test
    public void testFailsIfApplicationDoesntHavePersonSourceEnabled() throws Exception {
        String uri = "aUri";
        request.withParam("uri", uri);
        
        when(resolver.person(uri, application)).thenReturn(Optional.absent());
        
        peopleController.content(request, response);
        
        verify(outputter).writeError(argThat(is(request)), argThat(is(response)), any(AtlasErrorSummary.class));
        verify(outputter, never()).writeTo(argThat(is(request)), argThat(is(response)), anyPeople(), anySetOfPublishers(), any(Application.class));

    }


    @Test
    public void testResolvesPersonByIdWhenIdIsSupplied() throws Exception {
        String id = "cf2";
        request.withParam("id", id);
        
        Person person = new Person();
        person.setPublisher(Publisher.BBC);
        when(resolver.person(idCodec.decode(id).longValue(), application))
            .thenReturn(Optional.of(person));
        
        peopleController.content(request, response);
        
        verify(outputter, never()).writeError(argThat(is(request)), argThat(is(response)), any(AtlasErrorSummary.class));
        verify(outputter).writeTo(argThat(is(request)), argThat(is(response)), argThat(hasItems(person)), anySetOfPublishers(), any(Application.class));
        
    }

    @Test
    public void testResolvesPersonByUriWhenUriIsSupplied() throws Exception {
        String uri = "aUri";
        request.withParam("uri", uri);
        
        Person person = new Person();
        person.setPublisher(Publisher.BBC);
        when(resolver.person(uri, application)).thenReturn(Optional.of(person));
        
        peopleController.content(request, response);
        
        verify(outputter, never()).writeError(argThat(is(request)), argThat(is(response)), any(AtlasErrorSummary.class));
        verify(outputter).writeTo(argThat(is(request)), argThat(is(response)), argThat(hasItems(person)), anySetOfPublishers(), any(Application.class));
        
    }

    @SuppressWarnings("unchecked")
    private Set<Annotation> anySetOfPublishers() {
        return any(Set.class);
    }

    @SuppressWarnings("unchecked")
    private Iterable<Person> anyPeople() {
        return any(Iterable.class);
    }
}
