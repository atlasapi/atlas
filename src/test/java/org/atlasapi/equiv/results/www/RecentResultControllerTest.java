package org.atlasapi.equiv.results.www;

import org.atlasapi.AtlasWebModule;
import org.atlasapi.equiv.results.persistence.CombinedEquivalenceScore;
import org.atlasapi.equiv.results.persistence.RecentEquivalenceResultStore;
import org.atlasapi.equiv.results.persistence.StoredEquivalenceResult;

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
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.accept.ContentNegotiationManager;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.view;

@RunWith(MockitoJUnitRunner.class)
public class RecentResultControllerTest {

    @Mock private RecentEquivalenceResultStore recentEquivalenceResultStore;
    @Mock private ContentNegotiationManager contentNegotiationManager;
    @Mock private StoredEquivalenceResult storedEquivalenceResult;
    @Mock private CombinedEquivalenceScore combinedEquivalenceScore;
    @Mock private Table table;

    private MockMvc mockMvc;
    private AtlasWebModule webModule;
    private RecentResultController recentResultController;

    @Before
    public void setUp() throws Exception {
        this.webModule = new AtlasWebModule();
        this.recentResultController = new RecentResultController(recentEquivalenceResultStore);
        this.mockMvc = MockMvcBuilders.standaloneSetup(recentResultController)
                .setViewResolvers(webModule.viewResolver(contentNegotiationManager))
                .build();
    }

    @Test
    public void generatingCorrectOutputFromTemplateForRecentResultHtmlResponseOnEquivalenceResultsRecentEndpoint() throws Exception {
        generateContainerResults();
        generateItemResults();
        generateEquivalenceResult();

        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/system/equivalence/results/recent")
                .header("host", "localhost:80"))
                .andExpect(status().isOk())
                .andExpect(view().name("equivalence.recent"))
                .andReturn();


        String htmlOutput = IOUtils.toString(
                this.getClass().getResourceAsStream("/templating/system-equivalence-results-recent.html"),
                "UTF-8"
        );

        assertThat(result.getResponse().getContentAsString(), is(htmlOutput));
    }

    @Test
    public void generatingCorrectOutputFromTemplateForRecentResultHtmlResponseOnEquivalenceResultsRecentItemsEndpoint() throws Exception {
        generateItemResults();
        generateEquivalenceResult();

        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/system/equivalence/results/recent/items")
                .header("host", "localhost:80"))
                .andExpect(status().isOk())
                .andExpect(view().name("equivalence.recentItems"))
                .andReturn();


        String htmlOutput = IOUtils.toString(
                this.getClass().getResourceAsStream("/templating/system-equivalence-results-recent-items.html"),
                "UTF-8"
        );

        assertThat(result.getResponse().getContentAsString(), is(htmlOutput));
    }

    @Test
    public void generatingCorrectOutputFromTemplateForRecentResultHtmlResponseOnEquivalenceResultsRecentContainersEndpoint() throws Exception {
        generateContainerResults();
        generateEquivalenceResult();

        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/system/equivalence/results/recent/containers")
                .header("host", "localhost:80"))
                .andExpect(status().isOk())
                .andExpect(view().name("equivalence.recentContainers"))
                .andReturn();


        String htmlOutput = IOUtils.toString(
                this.getClass().getResourceAsStream("/templating/system-equivalence-results-recent-containers.html"),
                "UTF-8"
        );

        assertThat(result.getResponse().getContentAsString(), is(htmlOutput));
    }

    private void generateEquivalenceResult() {
        when(storedEquivalenceResult.id()).thenReturn("g62x");
        when(storedEquivalenceResult.title()).thenReturn("Peppa Pig");
        when(storedEquivalenceResult.resultTime()).thenReturn(DateTime.parse("2016-09-21T20:12:38Z"));
        when(storedEquivalenceResult.sourceResults()).thenReturn(table);
        when(table.columnKeySet()).thenReturn(ImmutableSet.of("Peppa Pig", "Peppa Pig", "Peppa Piggy 2"));
        when(storedEquivalenceResult.combinedResults()).thenReturn(ImmutableList.of(combinedEquivalenceScore));
        when(combinedEquivalenceScore.id()).thenReturn("xa7a");
        when(combinedEquivalenceScore.title()).thenReturn("Peppa Piggy", "Peppa Piggy 2");
        when(combinedEquivalenceScore.strong()).thenReturn(true);
        when(combinedEquivalenceScore.publisher()).thenReturn("pressassociation.com");
        when(combinedEquivalenceScore.score()).thenReturn(2D);
        when(storedEquivalenceResult.sourceResults()).thenReturn(table);
        when(table.row("g62x")).thenReturn(ImmutableMap.of("Description Title Matching Item Scorer", 2D, "Broadcast-Title-Subset", 2D));
    }

    private void generateItemResults() {
        when(recentEquivalenceResultStore.latestItemResults()).thenReturn(ImmutableList.of(storedEquivalenceResult, storedEquivalenceResult));
    }

    private void generateContainerResults() {
        when(recentEquivalenceResultStore.latestContainerResults()).thenReturn(ImmutableList.of(storedEquivalenceResult, storedEquivalenceResult));
    }
}