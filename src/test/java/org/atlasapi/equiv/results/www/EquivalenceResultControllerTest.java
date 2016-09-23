package org.atlasapi.equiv.results.www;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.AtlasWebModule;
import org.atlasapi.equiv.results.persistence.CombinedEquivalenceScore;
import org.atlasapi.equiv.results.persistence.EquivalenceResultStore;
import org.atlasapi.equiv.results.persistence.StoredEquivalenceResult;
import org.atlasapi.equiv.results.probe.EquivalenceProbeStore;
import org.atlasapi.equiv.results.probe.EquivalenceResultProbe;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.metabroadcast.common.base.Maybe;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Table;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.HttpMediaTypeNotAcceptableException;
import org.springframework.web.accept.ContentNegotiationManager;
import org.springframework.web.context.request.NativeWebRequest;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.view;

@RunWith(MockitoJUnitRunner.class)
public class EquivalenceResultControllerTest {

    @Mock private EquivalenceResultStore store;
    @Mock private EquivalenceProbeStore probeStore;
    @Mock private ContentResolver contentResolver;
    @Mock private LookupEntryStore lookupEntryStore;
    @Mock private HttpServletResponse response;
    @Mock private StoredEquivalenceResult equivalenceResult;
    @Mock private EquivalenceResultProbe probeResult;
    @Mock private Table table;
    @Mock private CombinedEquivalenceScore combinedEquivalenceScore;
    @Mock private ContentNegotiationManager contentNegotiationManager;
    @Mock private ResolvedContent resolvedContent;
    @Mock private Container container;
    @Mock private ChildRef childRef;
    private MockMvc mockMvc;
    private EquivalenceResultController equivalenceResultController;
    private String uri = "http://metabroadcast.com";
    private AtlasWebModule webModule;

    @Before
    public void setUp() throws Exception {
        this.webModule = new AtlasWebModule();
        this.equivalenceResultController = new EquivalenceResultController(
                store, probeStore, contentResolver, lookupEntryStore
        );

        this.mockMvc = MockMvcBuilders.standaloneSetup(equivalenceResultController)
                .setViewResolvers(webModule.viewResolver(contentNegotiationManager))
                .build();
    }

    @Test
    public void generatingCorrectOutputFromTemplateForStoredEquivalenceResultHtmlResponseOnResponseEndpoint() throws Exception {
        when(store.forId(uri)).thenReturn(equivalenceResult);
        when(probeStore.probeFor(uri)).thenReturn(probeResult);

        genereateEquivalenceResult();

        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get(String.format("/system/equivalence/result?uri=%s", uri))
                .header("host", "localhost:80"))
                .andExpect(status().isOk())
                .andExpect(view().name("equivalence.result"))
                .andReturn();

        String htmlOutput = IOUtils.toString(
                this.getClass().getResourceAsStream("/templating/system-equivalence-result.html"),
                "UTF-8"
        );

        assertThat(result.getResponse().getContentAsString(), is(htmlOutput));
    }

    private void genereateEquivalenceResult() throws HttpMediaTypeNotAcceptableException {
        when(equivalenceResult.id()).thenReturn("g62x");
        when(equivalenceResult.title()).thenReturn("Peppa Pig");
        when(equivalenceResult.resultTime()).thenReturn(DateTime.parse("2016-09-21T19:12:38Z"));
        when(equivalenceResult.sourceResults()).thenReturn(table);
        when(table.columnKeySet()).thenReturn(ImmutableSet.of("Peppa Pig", "Peppa Pig"));
        when(equivalenceResult.combinedResults()).thenReturn(ImmutableList.of(combinedEquivalenceScore));
        when(combinedEquivalenceScore.id()).thenReturn("wxrsf");
        when(combinedEquivalenceScore.title()).thenReturn("Peppa Pig");
        when(combinedEquivalenceScore.strong()).thenReturn(true);
        when(combinedEquivalenceScore.publisher()).thenReturn("pressassociation.com");
        when(combinedEquivalenceScore.title()).thenReturn("Peppa Pig");
        when(combinedEquivalenceScore.score()).thenReturn(2D);
        when(equivalenceResult.sourceResults()).thenReturn(table);
        when(table.row("wxrsf")).thenReturn(ImmutableMap.of("Description Title Matching Item Scorer", 2D, "Broadcast-Title-Subset", 2D));
        when(probeResult.expectedEquivalent()).thenReturn(ImmutableSet.of("wxrsf", "sdasd"));
        when(equivalenceResult.sourceResults()).thenReturn(table);
        when(table.columnKeySet()).thenReturn(ImmutableSet.of("Description Title Matching Item Scorer", "Broadcast-Title-Subset"));
        when(equivalenceResult.description()).thenReturn(ImmutableList.of("George and Peppa get caught outside in the middle of a big thunderstorm."));
        when(contentNegotiationManager.resolveMediaTypes(any(NativeWebRequest.class))).thenReturn(ImmutableList.of(
                MediaType.TEXT_HTML));
    }

    @Test
    public void generatingCorrectOutputFromTemplateForStoredEquivalenceResultHtmlResponseOnResponsesEndpoint() throws Exception {
        when(contentResolver.findByCanonicalUris(ImmutableList.of(uri))).thenReturn(resolvedContent);
        when(resolvedContent.get(uri)).thenReturn(Maybe.fromPossibleNullValue(container));
        when(container.getChildRefs()).thenReturn(ImmutableList.of(childRef, childRef));
        when(childRef.getUri()).thenReturn("http://metabroadcast.com/test1", "http://metabroadcast.com/test2");
        when(store.forIds(ImmutableList.of("http://metabroadcast.com/test1", "http://metabroadcast.com/test2"))).thenReturn(ImmutableList.of(equivalenceResult, equivalenceResult));
        when(probeStore.probeFor("http://metabroadcast.com/test1")).thenReturn(probeResult);
        when(probeStore.probeFor("http://metabroadcast.com/test2")).thenReturn(probeResult);

        genereateEquivalenceResult();

        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get(String.format("/system/equivalence/results?uri=%s", uri))
                .header("host", "localhost:80"))
                .andExpect(status().isOk())
                .andExpect(view().name("equivalence.results"))
                .andReturn();


        String htmlOutput = IOUtils.toString(
                this.getClass().getResourceAsStream("/templating/system-equivalence-results.html"),
                "UTF-8"
        );

        assertThat(result.getResponse().getContentAsString(), is(htmlOutput));
    }
}