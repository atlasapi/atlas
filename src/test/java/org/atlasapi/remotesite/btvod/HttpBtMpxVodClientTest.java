package org.atlasapi.remotesite.btvod;


import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.http.SimpleHttpRequest;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Iterator;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HttpBtMpxVodClientTest {


    @Mock
    private SimpleHttpClient httpClient;

    @Mock
    private HttpBtMpxFeedRequestProvider requestProvider;

    @InjectMocks
    private HttpBtMpxVodClient objectUnderTest;

    @Test
    public void testCorrectPagination() throws Exception {

        BtVodEntry btVodEntry1 = mock(BtVodEntry.class);
        BtVodEntry btVodEntry2 = mock(BtVodEntry.class);
        BtVodEntry btVodEntry3 = mock(BtVodEntry.class);
        BtVodEntry btVodEntry4 = mock(BtVodEntry.class);
        BtVodEntry btVodEntry5 = mock(BtVodEntry.class);


        BtVodResponse btVodResponse1 = mock(BtVodResponse.class);
        when(btVodResponse1.getStartIndex()).thenReturn(1);
        when(btVodResponse1.getItemsPerPage()).thenReturn(2);
        when(btVodResponse1.getEntryCount()).thenReturn(2);
        when(btVodResponse1.getEntries()).thenReturn(ImmutableList.of(btVodEntry1, btVodEntry2));

        BtVodResponse btVodResponse2 = mock(BtVodResponse.class);
        when(btVodResponse2.getStartIndex()).thenReturn(3);
        when(btVodResponse2.getItemsPerPage()).thenReturn(2);
        when(btVodResponse2.getEntryCount()).thenReturn(2);
        when(btVodResponse2.getEntries()).thenReturn(ImmutableList.of(btVodEntry3, btVodEntry4));


        BtVodResponse btVodResponse3 = mock(BtVodResponse.class);
        when(btVodResponse3.getStartIndex()).thenReturn(5);
        when(btVodResponse3.getItemsPerPage()).thenReturn(2);
        when(btVodResponse3.getEntryCount()).thenReturn(1);
        when(btVodResponse3.getEntries()).thenReturn(ImmutableList.of(btVodEntry5));


        SimpleHttpRequest<BtVodResponse> request1 = mock(SimpleHttpRequest.class);
        SimpleHttpRequest<BtVodResponse> request2 = mock(SimpleHttpRequest.class);
        SimpleHttpRequest<BtVodResponse> request3 = mock(SimpleHttpRequest.class);

        when(requestProvider.buildRequest(1)).thenReturn(request1);
        when(requestProvider.buildRequest(3)).thenReturn(request2);
        when(requestProvider.buildRequest(5)).thenReturn(request3);

        when(httpClient.get(request1)).thenReturn(btVodResponse1);
        when(httpClient.get(request2)).thenReturn(btVodResponse2);
        when(httpClient.get(request3)).thenReturn(btVodResponse3);


        Iterator<BtVodEntry> result = objectUnderTest.getBtMpxFeed();

        assertThat(result.next(), is(btVodEntry1));
        assertThat(result.next(), is(btVodEntry2));
        assertThat(result.next(), is(btVodEntry3));
        assertThat(result.next(), is(btVodEntry4));
        assertThat(result.next(), is(btVodEntry5));
        assertThat(result.hasNext(), is(false));


    }



}