package org.atlasapi.query.v2;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.query.ApiKeyNotFoundException;
import org.atlasapi.application.query.ApplicationConfigurationFetcher;
import org.atlasapi.application.query.InvalidIpForApiKeyException;
import org.atlasapi.application.query.RevokedApiKeyException;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.application.v3.SourceStatus;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.Schedule.ScheduleChannel;
import org.atlasapi.output.Annotation;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.NullAdapterLog;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.servlet.StubHttpServletRequest;
import com.metabroadcast.common.servlet.StubHttpServletResponse;
import com.metabroadcast.common.time.DateTimeZones;

@RunWith(MockitoJUnitRunner.class)
public class ScheduleControllerTest {
    
    private static final String NO_FROM = null;
    private static final String NO_TO = null;
    private static final String NO_COUNT = null;
    private static final String NO_ON = null;
    private static final String NO_CHANNEL_KEY = null;

    private final ScheduleResolver scheduleResolver = mock(ScheduleResolver.class);
    private final ChannelResolver channelResolver = mock(ChannelResolver.class);
    private final ApplicationConfigurationFetcher configFetcher = mock(ApplicationConfigurationFetcher.class);
    private final AdapterLog log = new NullAdapterLog();
    @SuppressWarnings("unchecked")
    private final AtlasModelWriter<Iterable<ScheduleChannel>> outputter = mock(AtlasModelWriter.class);
    private final ScheduleController controller = new ScheduleController(scheduleResolver, channelResolver, configFetcher, log, outputter);
    
    private DateTime to;
    private DateTime from;
    private StubHttpServletRequest request;
    private StubHttpServletResponse response;
    private Channel channel;
    
    @Before
    public void setup() throws ApiKeyNotFoundException, RevokedApiKeyException, InvalidIpForApiKeyException {
        from = new DateTime(DateTimeZones.UTC);
        to = new DateTime(DateTimeZones.UTC);
        request = new StubHttpServletRequest();
        response = new StubHttpServletResponse();
        channel = new Channel.Builder().build();
        
        when(configFetcher.configurationFor(request))
            .thenReturn(Maybe.<ApplicationConfiguration>nothing());
        when(channelResolver.fromId(any(Long.class)))
            .thenReturn(Maybe.just(channel));
    }
    
    @Test
    public void testScheduleRequestFailsWithNoPublishersOrApiKey() throws IOException {
        
        String NO_PUBLISHERS = null;
        controller.schedule(from.toString(), to.toString(), NO_COUNT, NO_ON, NO_CHANNEL_KEY, "cid", NO_PUBLISHERS, request, response);
        
        verify(outputter).writeError(argThat(is(request)), argThat(is(response)), any(AtlasErrorSummary.class));
        verify(outputter, never()).writeTo(argThat(is(request)), argThat(is(response)), anyChannelSchedules(), anySetOfPublishers(), any(ApplicationConfiguration.class));
    }

    @Test
    public void testScheduleRequestPassWithJustPublishers() throws IOException {
        
        when(scheduleResolver.schedule(eq(from), eq(to), argThat(hasItems(channel)), argThat(hasItems(Publisher.BBC)), eq(Optional.<ApplicationConfiguration>absent())))
            .thenReturn(Schedule.fromChannelMap(ImmutableMap.<Channel,List<Item>>of(), new Interval(from, to)));
        
        controller.schedule(from.toString(), to.toString(), NO_COUNT, NO_ON, NO_CHANNEL_KEY, "cbbh", "bbc.co.uk", request, response);
        
        verify(outputter).writeTo(argThat(is(request)), argThat(is(response)), anyChannelSchedules(), anySetOfPublishers(), any(ApplicationConfiguration.class));
    }

    @Test
    public void testScheduleRequestWithOnParameter() throws IOException {
        
        when(scheduleResolver.schedule(eq(from), eq(to), argThat(hasItems(channel)), argThat(hasItems(Publisher.BBC)), eq(Optional.<ApplicationConfiguration>absent())))
            .thenReturn(Schedule.fromChannelMap(ImmutableMap.<Channel,List<Item>>of(), new Interval(from, to)));
        
        controller.schedule(NO_FROM, NO_TO, NO_COUNT, from.toString(), NO_CHANNEL_KEY, "cbbh", "bbc.co.uk", request, response);
        
        verify(outputter).writeTo(argThat(is(request)), argThat(is(response)), anyChannelSchedules(), anySetOfPublishers(), any(ApplicationConfiguration.class));
    }
    
    @Test
    public void testScheduleRequestWithCountParameter() throws IOException {
        
        int count = 10;
        when(scheduleResolver.schedule(eq(from), eq(count), argThat(hasItems(channel)), argThat(hasItems(Publisher.BBC)), eq(Optional.<ApplicationConfiguration>absent())))
            .thenReturn(Schedule.fromChannelMap(ImmutableMap.<Channel,List<Item>>of(), new Interval(from, to)));
        
        controller.schedule(from.toString(), NO_TO, String.valueOf(count), NO_ON, NO_CHANNEL_KEY, "cbbh", "bbc.co.uk", request, response);
        
        verify(outputter).writeTo(argThat(is(request)), argThat(is(response)), anyChannelSchedules(), anySetOfPublishers(), any(ApplicationConfiguration.class));
    }

    @Test
    public void testErrorsWhenParamsAreMissing() throws Exception {

        controller.schedule(NO_FROM, NO_TO, NO_COUNT, NO_ON, NO_CHANNEL_KEY, "cbbh", "bbc.co.uk", request, response);
        
        verify(outputter, never()).writeTo(argThat(is(request)), argThat(is(response)), anyChannelSchedules(), anySetOfPublishers(), any(ApplicationConfiguration.class));
        verifyExceptionThrownAndWrittenToUser(IllegalArgumentException.class);
    }
    
    @Test
    public void testErrorsWhenToAndCountAreSuppliedWithFrom() throws Exception {
        
        controller.schedule(from.toString(), from.toString(), "5", NO_ON, NO_CHANNEL_KEY, "cbbh", "bbc.co.uk", request, response);
        
        verify(outputter, never()).writeTo(argThat(is(request)), argThat(is(response)), anyChannelSchedules(), anySetOfPublishers(), any(ApplicationConfiguration.class));
        verifyExceptionThrownAndWrittenToUser(IllegalArgumentException.class);
        
    }

    @Test
    public void testErrorsWhenOnlyFromSupplied() throws Exception {
        
        controller.schedule(from.toString(), NO_TO, NO_COUNT, NO_ON, NO_CHANNEL_KEY, "cbbh", "bbc.co.uk", request, response);
        
        verify(outputter, never()).writeTo(argThat(is(request)), argThat(is(response)), anyChannelSchedules(), anySetOfPublishers(), any(ApplicationConfiguration.class));
        verifyExceptionThrownAndWrittenToUser(IllegalArgumentException.class);
        
    }

    @Test
    public void testErrorsWhenOnlyToSupplied() throws Exception {
        
        controller.schedule(NO_FROM, to.toString(), NO_COUNT, NO_ON, NO_CHANNEL_KEY, "cbbh", "bbc.co.uk", request, response);
        
        verify(outputter, never()).writeTo(argThat(is(request)), argThat(is(response)), anyChannelSchedules(), anySetOfPublishers(), any(ApplicationConfiguration.class));
        verifyExceptionThrownAndWrittenToUser(IllegalArgumentException.class);
        
    }

    @Test
    public void testErrorsWhenOnlyCountSupplied() throws Exception {
        
        controller.schedule(NO_FROM, NO_TO, "5", NO_ON, NO_CHANNEL_KEY, "cbbh", "bbc.co.uk", request, response);
        
        verify(outputter, never()).writeTo(argThat(is(request)), argThat(is(response)), anyChannelSchedules(), anySetOfPublishers(), any(ApplicationConfiguration.class));
        verifyExceptionThrownAndWrittenToUser(IllegalArgumentException.class);
        
    }

    @Test
    public void testErrorsWhenApiKeyForUnknownAppIsSupplied() throws Exception {
        HttpServletRequest req = request.withParam("apiKey", "unknownKey");
        when(configFetcher.configurationFor(req)).thenThrow(RevokedApiKeyException.class);

        controller.schedule(from.toString(), NO_TO, "5", NO_ON, NO_CHANNEL_KEY, "cbbh", "bbc.co.uk", req, response);
        verifyExceptionThrownAndWrittenToUser(RevokedApiKeyException.class);
    }

    @Test
    public void testErrorsWhenNoPublishersSupplied() throws Exception {
        
        controller.schedule(from.toString(), NO_TO, "5", NO_ON, NO_CHANNEL_KEY, "cbbh", "not_a_publisher", request, response);
        
        verify(outputter, never()).writeTo(argThat(is(request)), argThat(is(response)), anyChannelSchedules(), anySetOfPublishers(), any(ApplicationConfiguration.class));
        verifyExceptionThrownAndWrittenToUser(IllegalArgumentException.class);
        
    }
    
    @Test
    public void testErrorsWhenChannelAndChannelIdAreBothSupplied() throws Exception {
        
        controller.schedule(from.toString(), NO_TO, "5", NO_ON, "bbcone", "cbbh", "bbc.co.uk", request, response);
        
        verify(outputter, never()).writeTo(argThat(is(request)), argThat(is(response)), anyChannelSchedules(), anySetOfPublishers(), any(ApplicationConfiguration.class));
        verifyExceptionThrownAndWrittenToUser(IllegalArgumentException.class);
        
    }

    @Test
    public void testErrorsWhenMissingChannelIsSupplied() throws Exception {
        
        when(channelResolver.fromId(any(Long.class)))
            .thenReturn(Maybe.<Channel>nothing());
        
        controller.schedule(from.toString(), NO_TO, "5", NO_ON, NO_CHANNEL_KEY, "cbbh", "bbc.co.uk", request, response);
        
        verify(outputter, never()).writeTo(argThat(is(request)), argThat(is(response)), anyChannelSchedules(), anySetOfPublishers(), any(ApplicationConfiguration.class));
        verifyExceptionThrownAndWrittenToUser(IllegalArgumentException.class);
        
    }

    @Test
    public void testPassesAppConfigToResolverWhenNoPublishersSupplied() throws Exception {
        
        HttpServletRequest req = request.withParam("apiKey", "key");
        ApplicationConfiguration appConfig = ApplicationConfiguration.defaultConfiguration()
                .copyWithPrecedence(ImmutableList.<Publisher>of());
        
        when(configFetcher.configurationFor(req))
            .thenReturn(Maybe.just(appConfig));
        when(scheduleResolver.schedule(eq(from), eq(5), argThat(hasItems(channel)), argThat(hasItems(Publisher.BBC)), eq(Optional.of(appConfig))))
            .thenReturn(Schedule.fromChannelMap(ImmutableMap.<Channel,List<Item>>of(), new Interval(from, from)));
        
        String NO_PUBLISHERS = null;
        controller.schedule(from.toString(), NO_TO, "5", NO_ON, NO_CHANNEL_KEY, "cbbh", NO_PUBLISHERS, req, response);
        
        verify(outputter).writeTo(argThat(is(req)), argThat(is(response)), anyChannelSchedules(), anySetOfPublishers(), argThat(is(appConfig)));
        
    }
    
    @Test
    public void testDoesntPassAppConfigToResolverWhenPublishersSuppliedWithApiKey() throws Exception {
        
        HttpServletRequest req = request.withParam("apiKey", "key");
        ApplicationConfiguration appConfig = ApplicationConfiguration.defaultConfiguration()
                .withSource(Publisher.BBC, SourceStatus.AVAILABLE_ENABLED)
                .copyWithPrecedence(ImmutableList.<Publisher>of());
        
        HttpServletRequest matchRequest = req;
        when(configFetcher.configurationFor(matchRequest))
            .thenReturn(Maybe.just(appConfig));
        when(scheduleResolver.schedule(eq(from), eq(5), argThat(hasItems(channel)), argThat(hasItems(Publisher.BBC)), eq(Optional.<ApplicationConfiguration>absent())))
            .thenReturn(Schedule.fromChannelMap(ImmutableMap.<Channel,List<Item>>of(), new Interval(from, from)));
        
        controller.schedule(from.toString(), NO_TO, "5", NO_ON, NO_CHANNEL_KEY, "cbbh", "bbc.co.uk", matchRequest, response);
        
        verify(outputter).writeTo(argThat(is(matchRequest)), argThat(is(response)), anyChannelSchedules(), anySetOfPublishers(), argThat(is(appConfig)));
        
    }
    
    @Test
    public void testResolvesChannelByKey() throws Exception {
        
        when(channelResolver.fromKey(any(String.class))).thenReturn(Maybe.just(channel));
        when(scheduleResolver.schedule(eq(from), eq(5), argThat(hasItems(channel)), argThat(hasItems(Publisher.BBC)), eq(Optional.<ApplicationConfiguration>absent())))
        .thenReturn(Schedule.fromChannelMap(ImmutableMap.<Channel,List<Item>>of(), new Interval(from, from)));
        
        controller.schedule(from.toString(), NO_TO, "5", NO_ON, "bbcone", null, "bbc.co.uk", request, response);
        
        verify(outputter).writeTo(argThat(is(request)), argThat(is(response)), anyChannelSchedules(), anySetOfPublishers(), any(ApplicationConfiguration.class));
        
    }
    
    @Test
    public void testErrorsWhenCountIsNotPositive() throws Exception {
        
        controller.schedule(from.toString(), NO_TO, "0", NO_ON, NO_CHANNEL_KEY, "cbbh", "bbc.co.uk", request, response);
        
        verify(outputter, never()).writeTo(argThat(is(request)), argThat(is(response)), anyChannelSchedules(), anySetOfPublishers(), any(ApplicationConfiguration.class));
        verifyExceptionThrownAndWrittenToUser(IllegalArgumentException.class);
        
    }

    @Test
    public void testErrorsWhenCountIsAboveMax() throws Exception {
        
        controller.schedule(from.toString(), NO_TO, "11", NO_ON, NO_CHANNEL_KEY, "cbbh", "bbc.co.uk", request, response);
        
        verify(outputter, never()).writeTo(argThat(is(request)), argThat(is(response)), anyChannelSchedules(), anySetOfPublishers(), any(ApplicationConfiguration.class));
        verifyExceptionThrownAndWrittenToUser(IllegalArgumentException.class);
        
    }

    @SuppressWarnings("unchecked")
    private Set<Annotation> anySetOfPublishers() {
        return any(Set.class);
    }

    @SuppressWarnings("unchecked")
    private Iterable<Schedule.ScheduleChannel> anyChannelSchedules() {
        return any(Iterable.class);
    }
    

    private void verifyExceptionThrownAndWrittenToUser(Class<? extends Exception> expectedException) throws IOException {
        ArgumentCaptor<AtlasErrorSummary> errorCaptor = ArgumentCaptor.forClass(AtlasErrorSummary.class);
        verify(outputter).writeError(argThat(is(request)), argThat(is(response)), errorCaptor.capture());
        assertThat(errorCaptor.getValue().exception(), is(instanceOf(expectedException)));
    }
    
}
