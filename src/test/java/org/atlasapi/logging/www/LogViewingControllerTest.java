package org.atlasapi.logging.www;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.atlasapi.AtlasWebModule;
import org.atlasapi.persistence.logging.AdapterLogEntry;
import org.atlasapi.persistence.logging.LogReader;
import org.atlasapi.remotesite.archiveorg.ArchiveOrgItemAdapter;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.view;

@RunWith(MockitoJUnitRunner.class)
public class LogViewingControllerTest {

    @Mock
    private LogReader logReader;
    @Mock
    private ContentNegotiationManager contentNegotiationManager;

    private AdapterLogEntry adapterLogEntry;
    private LogViewingController logViewingController;
    private MockMvc mockMvc;
    private AtlasWebModule webModule;
    private String errorId = "1234-test";

    @Before
    public void setUp() throws Exception {
        this.webModule = new AtlasWebModule();
        this.logViewingController = new LogViewingController(
                logReader
        );

        this.mockMvc = MockMvcBuilders.standaloneSetup(logViewingController)
                .setViewResolvers(webModule.viewResolver(contentNegotiationManager))
                .build();
        this.adapterLogEntry = new AdapterLogEntry("testId", AdapterLogEntry.Severity.INFO, DateTime.parse("2016-09-21T19:12:38Z"))
                .withCause(new Exception("test"))
                .withUri(String.format("/system/log/%s/trace", errorId))
                .withSource(ArchiveOrgItemAdapter.class);
    }

    @Test
    public void correctLogTraceHtmlOutputIsGeneratedFromTemplate() throws Exception {
        when(logReader.requireById(errorId)).thenReturn(adapterLogEntry);

        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get(String.format("/system/log/%s/trace", errorId))
                .header("host", "localhost:80"))
                .andExpect(status().isOk())
                .andExpect(view().name("system/log/trace"))
                .andReturn();

        String[] lines = result.getResponse().getContentAsString().split(System.getProperty("line.separator"));
        List<String> log = new ArrayList<>(Arrays.asList(lines));

        assertTrue(log.size() > 2);
        assertThat(log.get(0), is("<html><head><title>Logs</title></head><body><pre>java.lang.Exception :test"));
        assertThat(log.get(log.size() - 1), is("</pre></body></html>"));
    }

    @Test
    public void correctInfoLevelLogShowHtmlOutputIsGeneratedFromTemplate() throws Exception {
        when(logReader.read()).thenReturn(ImmutableList.of(adapterLogEntry));

        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/system/log")
                .header("host", "localhost:80"))
                .andExpect(status().isOk())
                .andExpect(view().name("system/log/show"))
                .andReturn();

        String htmlOutput = IOUtils.toString(
                this.getClass().getResourceAsStream("/templating/system-log-show.html"),
                "UTF-8"
        );

        assertThat(result.getResponse().getContentAsString(), is(htmlOutput));
    }
}