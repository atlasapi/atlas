package org.atlasapi.remotesite.bbc.nitro;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelType;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.atlas.glycerin.Glycerin;
import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.GlycerinResponse;
import com.metabroadcast.atlas.glycerin.model.DateRange;
import com.metabroadcast.atlas.glycerin.model.Id;
import com.metabroadcast.atlas.glycerin.model.Ids;
import com.metabroadcast.atlas.glycerin.model.Service;
import com.metabroadcast.atlas.glycerin.queries.ServicesQuery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.sun.org.apache.xerces.internal.jaxp.datatype.XMLGregorianCalendarImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GlycerinNitroChannelAdapterTest {

    @Mock
    private Glycerin glycerin;
    @Mock
    private GlycerinResponse response;
    private GlycerinNitroChannelAdapter channelAdapter;

    @Before
    public void setUp() throws GlycerinException {
        when(glycerin.execute(any(ServicesQuery.class))).thenReturn(response);
        channelAdapter = GlycerinNitroChannelAdapter.create(glycerin);
    }

    @Test
    public void generatesAtlasModelFromNitroServices() throws GlycerinException {
        Service service = getBasicService();
        setIds(service);
        setDateRange(service);
        when(response.getResults()).thenReturn(ImmutableList.of(service));
        ImmutableSet<Channel> services = channelAdapter.fetchServices();
        Channel channel = Iterables.getOnlyElement(services);
        assertThat(channel.getChannelType(), is(ChannelType.CHANNEL));
        assertThat(channel.getRegion(), is("ALL"));
        assertThat(channel.getUri(), is("http://nitro.bbc.co.uk/bbc_radio_fourlw"));
        assertThat(channel.getMediumDescription(), is("description"));
        assertThat(channel.getMediaType(), is(MediaType.AUDIO));
        assertThat(channel.getTitle(), is("name"));
        assertThat(channel.getSource(), is(Publisher.BBC_NITRO));
        assertThat(channel.getBroadcaster(), is(Publisher.BBC));
        assertNotNull(channel.getEndDate());
        assertNotNull(channel.getStartDate());
    }

    @Test
    public void doesntFailOnNonCompulsoryFields() throws GlycerinException {
        Service service = getBasicService();
        setIds(service);
        when(response.getResults()).thenReturn(ImmutableList.of(service));
        ImmutableSet<Channel> services = channelAdapter.fetchServices();
        Channel channel = Iterables.getOnlyElement(services);
        assertThat(channel.getChannelType(), is(ChannelType.CHANNEL));
        assertThat(channel.getRegion(), is("ALL"));
        assertThat(channel.getUri(), is("http://nitro.bbc.co.uk/bbc_radio_fourlw"));
        assertThat(channel.getMediumDescription(), is("description"));
        assertThat(channel.getMediaType(), is(MediaType.AUDIO));
        assertThat(channel.getTitle(), is("name"));
        assertThat(channel.getSource(), is(Publisher.BBC_NITRO));
        assertThat(channel.getBroadcaster(), is(Publisher.BBC));
    }

    private void setDateRange(Service service) {
        DateRange dateRange = new DateRange();
        dateRange.setEnd(XMLGregorianCalendarImpl.createDate(2000,1,1,1));
        dateRange.setStart(XMLGregorianCalendarImpl.createDate(2000,1,1,1));
        service.setDateRange(dateRange);
    }

    private void setIds(Service service) {
        Ids ids = new Ids();
        Id id = new Id();
        id.setType("pid");
        id.setValue("p00fzl64");
        ids.getId().add(id);
        Id id2 = new Id();
        id2.setType("terrestrial_service_locator");
        id2.setValue("dvb://233a..1700");
        ids.getId().add(id2);
        service.setIds(ids);
    }

    private Service getBasicService() {
        Service service = new Service();
        service.setSid("bbc_radio_fourlw");
        service.setName("name");
        service.setDescription("description");
        service.setRegion("ALL");
        service.setMediaType("Audio");
        return service;
    }

}