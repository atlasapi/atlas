package org.atlasapi.query.v2;

import java.io.IOException;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.query.ApplicationFetcher;
import org.atlasapi.application.query.InvalidApiKeyException;
import org.atlasapi.input.ChannelGroupTransformer;
import org.atlasapi.input.DefaultJacksonModelReader;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.persistence.logging.AdapterLog;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;

import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChannelGroupControllerTest {

    private ChannelGroupController controller;
    private ChannelGroupResolver channelGroupResolver;
    private ChannelGroup channelGroup;
    private ChannelGroupWriteExecutor executor;
    private ApplicationFetcher applicationFetcher;
    private Application application;
    private ApplicationConfiguration configuration;
    private HttpServletRequest request;
    private HttpServletResponse response;

    @Before
    public void setUp() {
        AtlasModelWriter<Iterable<ChannelGroup>> atlasModelWriter = mock(AtlasModelWriter.class);
        AdapterLog adapterLog = mock(AdapterLog.class);
        ChannelGroupTransformer channelGroupTransformer = mock(ChannelGroupTransformer.class);
        ChannelResolver channelResolver = mock(ChannelResolver.class);

        channelGroup = mock(ChannelGroup.class);
        channelGroupResolver = mock(ChannelGroupResolver.class);
        executor = mock(ChannelGroupWriteExecutor.class);
        applicationFetcher = mock(ApplicationFetcher.class);
        application = mock(Application.class);
        configuration = mock(ApplicationConfiguration.class);
        request = mock(HttpServletRequest.class);
        response = mock(HttpServletResponse.class);

        controller = ChannelGroupController.builder()
                .withApplicationFetcher(applicationFetcher)
                .withLog(adapterLog)
                .withAtlasModelWriter(atlasModelWriter)
                .withModelReader(new DefaultJacksonModelReader())
                .withChannelGroupResolver(channelGroupResolver)
                .withChannelGroupTransformer(channelGroupTransformer)
                .withChannelGroupWriteExecutor(executor)
                .withChannelResolver(channelResolver)
                .build();
    }

    @Test
    public void testClientDoesntHavePermissionsToDelete() throws InvalidApiKeyException, IOException {
        when(applicationFetcher.applicationFor(request)).thenReturn(Optional.ofNullable(application));
        when(channelGroupResolver.channelGroupFor(1L)).thenReturn(com.google.common.base.Optional.fromNullable(channelGroup));
        when(application.getConfiguration()).thenReturn(configuration);
        when(configuration.isWriteEnabled(any())).thenReturn(false);

        controller.deleteChannelGroup("bc", request, response);

        verify(executor, times(0)).deletePlatform(request, response, 1L);
    }

    @Test
    public void testClientHasPermissionsToDelete() throws InvalidApiKeyException, IOException {
        when(applicationFetcher.applicationFor(any())).thenReturn(Optional.ofNullable(application));
        when(channelGroupResolver.channelGroupFor(1L))
                .thenReturn(com.google.common.base.Optional.fromNullable(channelGroup));
        when(application.getConfiguration()).thenReturn(configuration);
        when(configuration.isWriteEnabled(any())).thenReturn(true);
        when(executor.deletePlatform(request, response, 1L))
                .thenReturn(Optional.empty());

        controller.deleteChannelGroup("bc", request, response);

        verify(executor, times(1)).deletePlatform(request, response, 1L);
    }
}