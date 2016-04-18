package org.atlasapi.query.v2;

import java.io.IOException;
import java.math.BigInteger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.query.ApiKeyNotFoundException;
import org.atlasapi.application.query.ApplicationConfigurationFetcher;
import org.atlasapi.application.query.InvalidIpForApiKeyException;
import org.atlasapi.application.query.RevokedApiKeyException;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.persistence.logging.AdapterLog;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.NumberToShortStringCodec;

import jdk.nashorn.internal.runtime.NumberToString;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChannelControllerTest {

    private ChannelController channelController;
    private ApplicationConfigurationFetcher configurationFetcher = mock(ApplicationConfigurationFetcher.class);
    private AdapterLog adapterLog = mock(AdapterLog.class);
    private AtlasModelWriter atlasModelWriter = mock(AtlasModelWriter.class);
    private ChannelResolver channelResolver = mock(ChannelResolver.class);
    private NumberToShortStringCodec codec = mock(NumberToShortStringCodec.class);
    private HttpServletResponse response = mock(HttpServletResponse.class);
    private HttpServletRequest request = mock(HttpServletRequest.class);
    private Channel channel = mock(Channel.class);


    @Before
    public void setUp() throws Exception {
        this.channelController = new ChannelController(configurationFetcher,
                adapterLog,
                atlasModelWriter,
                channelResolver,
                codec);
    }

    @Test
    public void testIfRevokedAmvn cleanpiKeyExceptionIsThrownWhenApiKeyAccessHasBeenRevoked()
            throws IOException, InvalidIpForApiKeyException, ApiKeyNotFoundException,
            RevokedApiKeyException {
        String id = "dsad3";
        BigInteger integerValue = BigInteger.ONE;
        Long longValue = 1l;

        when(codec.decode(id)).thenReturn(integerValue);
        when(integerValue.longValue()).thenReturn(longValue);
        when(channelResolver.fromId(longValue)).thenReturn(Maybe.just(channel));
        when(configurationFetcher.configurationFor(request)).thenThrow(RevokedApiKeyException.class);

        channelController.listChannel(request, response, id);
    }

//            final ApplicationConfiguration appConfig;
//            try {
//                appConfig = appConfig(request);
//            } catch (ApiKeyNotFoundException | RevokedApiKeyException | InvalidIpForApiKeyException ex) {
//                outputter.writeError(request, response, FORBIDDEN);
//                return;
//            }
}