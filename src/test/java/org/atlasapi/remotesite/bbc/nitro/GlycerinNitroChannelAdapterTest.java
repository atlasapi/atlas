package org.atlasapi.remotesite.bbc.nitro;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelType;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.atlas.glycerin.Glycerin;
import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.GlycerinResponse;
import com.metabroadcast.atlas.glycerin.model.Brand;
import com.metabroadcast.atlas.glycerin.model.DateRange;
import com.metabroadcast.atlas.glycerin.model.Id;
import com.metabroadcast.atlas.glycerin.model.Ids;
import com.metabroadcast.atlas.glycerin.model.MasterBrand;
import com.metabroadcast.atlas.glycerin.model.Service;
import com.metabroadcast.atlas.glycerin.queries.MasterBrandsQuery;
import com.metabroadcast.atlas.glycerin.queries.ServicesQuery;
import com.metabroadcast.columbus.telescope.client.ModelWithPayload;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GlycerinNitroChannelAdapterTest {

    private static final String TERRESTRIAL_LOCATOR = "dvb://233A..1700";
    @Mock private Glycerin glycerin;
    @Mock private GlycerinResponse response;
    private GlycerinNitroChannelAdapter channelAdapter;

    @Before
    public void setUp() throws GlycerinException {
        when(glycerin.execute(any(ServicesQuery.class))).thenReturn(response);
        when(glycerin.execute(any(MasterBrandsQuery.class))).thenReturn(response);
        channelAdapter = GlycerinNitroChannelAdapter.create(glycerin);
    }

    @Test
    public void generatesAtlasModelFromNitroServices() throws GlycerinException {
        Service service = getBasicService();
        setIds(service);
        setDateRange(service);

        when(response.getResults()).thenReturn(ImmutableList.of(service));

        Image image = new Image("uri");
        image.setAliases(ImmutableList.of(new Alias("bbc:service:name:short", "name")));

        ImmutableList<ModelWithPayload<Channel>> servicesWithPayloads = channelAdapter.fetchServices(
                ImmutableMap.of(
                        "http://nitro.bbc.co.uk/masterbrands/bbc_radio_fourlw",
                        Channel.builder()
                        .withUri("http://nitro.bbc.co.uk/masterbrand/bbc_radio_fourlw")
                        .withImage(image)
                        .withTitle("parent")
                        .build()
                )
        );

        ModelWithPayload<Channel> channelWithPayload = servicesWithPayloads.stream()
                .filter(chan -> chan.getModel().getCanonicalUri() != null)
                .findFirst()
                .get();

        Channel channel = channelWithPayload.getModel();

        assertThat(channel.getChannelType(), is(ChannelType.CHANNEL));
        assertThat(channel.getRegion(), is("ALL"));
        assertThat(
                channel.getUri(),
                is("http://nitro.bbc.co.uk/services/bbc_radio_fourlw_233a_1700")
        );
        assertThat(channel.getMediumDescription(), is("description"));
        assertThat(channel.getMediaType(), is(MediaType.AUDIO));
        assertThat(channel.getTitle(), is("name"));
        assertThat(channel.getSource(), is(Publisher.BBC_NITRO));
        assertThat(channel.getBroadcaster(), is(Publisher.BBC));
        assertNotNull(channel.getEndDate());
        assertNotNull(channel.getStartDate());
        assertThat(channel.getImages().isEmpty(), is(false));
        assertThat(channel.getImages().iterator().next().getAliases().isEmpty(), is(false));
        assertThat(channel.getAliases().isEmpty(), is(false));
        assertThat(Iterables.isEmpty(Iterables.filter(channel.getAliases(),
                alias -> "bbc:service:name:short".equals(alias.getNamespace())
                        && "parent".equals(alias.getValue())
        )), is(false));
    }

    @Test
    public void generatesAtlasModelFromNitroMasterbrands() throws GlycerinException {
        MasterBrand masterBrand = new MasterBrand();
        masterBrand.setMid("bbc_radio_fourlw");
        masterBrand.setTitle("name");

        when(response.getResults()).thenReturn(ImmutableList.of(masterBrand));
        ImmutableSet<ModelWithPayload<Channel>> services = channelAdapter.fetchMasterbrands();
        ModelWithPayload<Channel> channelWithPayload = Iterables.getOnlyElement(services);
        Channel channel = channelWithPayload.getModel();
        assertThat(channel.getChannelType(), is(ChannelType.MASTERBRAND));
        assertThat(channel.getUri(), is("http://nitro.bbc.co.uk/masterbrands/bbc_radio_fourlw"));
        assertThat(channel.getTitle(), is("name"));
        assertThat(channel.getSource(), is(Publisher.BBC_NITRO));
        assertThat(channel.getBroadcaster(), is(Publisher.BBC));
    }

    @Test
    public void prefersTitleOverNameForMasterbrands() throws GlycerinException {
        MasterBrand masterBrand = new MasterBrand();
        masterBrand.setMid("bbc_radio_fourlw");
        masterBrand.setName("name");
        masterBrand.setTitle("title");

        when(response.getResults()).thenReturn(ImmutableList.of(masterBrand));
        ImmutableSet<ModelWithPayload<Channel>> services = channelAdapter.fetchMasterbrands();
        ModelWithPayload<Channel> channelWithPayload = Iterables.getOnlyElement(services);
        Channel channel = channelWithPayload.getModel();
        assertThat(channel.getChannelType(), is(ChannelType.MASTERBRAND));
        assertThat(channel.getUri(), is("http://nitro.bbc.co.uk/masterbrands/bbc_radio_fourlw"));
        assertThat(channel.getTitle(), is("title"));
        assertThat(channel.getSource(), is(Publisher.BBC_NITRO));
        assertThat(channel.getBroadcaster(), is(Publisher.BBC));
    }

    @Test
    public void fallsBackToNameIfTitleNotPresentForMasterbrands() throws GlycerinException {
        MasterBrand masterBrand = new MasterBrand();
        masterBrand.setMid("bbc_radio_fourlw");
        masterBrand.setName("name");

        when(response.getResults()).thenReturn(ImmutableList.of(masterBrand));
        ImmutableSet<ModelWithPayload<Channel>> services = channelAdapter.fetchMasterbrands();
        ModelWithPayload<Channel> channelWithPayload = Iterables.getOnlyElement(services);
        Channel channel = channelWithPayload.getModel();
        assertThat(channel.getChannelType(), is(ChannelType.MASTERBRAND));
        assertThat(channel.getUri(), is("http://nitro.bbc.co.uk/masterbrands/bbc_radio_fourlw"));
        assertThat(channel.getTitle(), is("name"));
        assertThat(channel.getSource(), is(Publisher.BBC_NITRO));
        assertThat(channel.getBroadcaster(), is(Publisher.BBC));
    }

    @Test
    public void masterbrandsWithRadioInTitle() throws GlycerinException {
        MasterBrand masterBrand = new MasterBrand();
        masterBrand.setMid("bbc_derp");
        masterBrand.setTitle("BBC Radio Four");

        when(response.getResults()).thenReturn(ImmutableList.of(masterBrand));
        ImmutableSet<ModelWithPayload<Channel>> services = channelAdapter.fetchMasterbrands();
        ModelWithPayload<Channel> channelWithPayload = Iterables.getOnlyElement(services);
        Channel channel = channelWithPayload.getModel();

        assertThat(channel.getChannelType(), is(ChannelType.MASTERBRAND));
        assertThat(channel.getMediaType(), is(MediaType.AUDIO));
    }

    @Test
    public void masterbrandsWithRadioInMid() throws GlycerinException {
        MasterBrand masterBrand = new MasterBrand();
        masterBrand.setMid("bbc_radio_fourlw");
        masterBrand.setTitle("BBC Non Latin Name");

        when(response.getResults()).thenReturn(ImmutableList.of(masterBrand));
        ImmutableSet<ModelWithPayload<Channel>> services = channelAdapter.fetchMasterbrands();
        ModelWithPayload<Channel> channelWithPayload = Iterables.getOnlyElement(services);
        Channel channel = channelWithPayload.getModel();

        assertThat(channel.getChannelType(), is(ChannelType.MASTERBRAND));
        assertThat(channel.getMediaType(), is(MediaType.AUDIO));
    }

    @Test
    public void generatesAtlasModelFromAdditionalMasterbrandsFields() throws GlycerinException {
        MasterBrand masterBrand = new MasterBrand();
        masterBrand.setMid("bbc_radio_fourlw");
        masterBrand.setName("name");
        masterBrand.setTitle("name");

        setImages(masterBrand);
        setSynopses(masterBrand);
        when(response.getResults()).thenReturn(ImmutableList.of(masterBrand));
        ImmutableSet<ModelWithPayload<Channel>> services = channelAdapter.fetchMasterbrands();
        ModelWithPayload<Channel> channelWithPayload = Iterables.getOnlyElement(services);
        Channel channel = channelWithPayload.getModel();

        assertThat(channel.getChannelType(), is(ChannelType.MASTERBRAND));
        assertThat(channel.getUri(), is("http://nitro.bbc.co.uk/masterbrands/bbc_radio_fourlw"));
        assertThat(channel.getTitle(), is("name"));
        assertThat(channel.getSource(), is(Publisher.BBC_NITRO));
        assertThat(channel.getBroadcaster(), is(Publisher.BBC));
        assertThat(channel.getShortDescription(), is("short"));
        assertThat(channel.getMediumDescription(), is("medium"));
        assertThat(channel.getLongDescription(), is("long"));
        assertThat(channel.getImages().isEmpty(), is(false));
        assertThat(channel.getImages().iterator().next().getAliases().isEmpty(), is(false));
    }

    @Test
    public void doesntFailOnNonCompulsoryFields() throws GlycerinException {
        Service service = getBasicService();
        setIds(service);

        when(response.getResults()).thenReturn(ImmutableList.of(service));

        ImmutableList<ModelWithPayload<Channel>> services = channelAdapter.fetchServices();
        ModelWithPayload<Channel> channelWithPayload = services.stream()
                .filter(chan -> chan.getModel().getCanonicalUri() != null)
                .findFirst()
                .get();
        Channel channel = channelWithPayload.getModel();

        assertThat(channel.getChannelType(), is(ChannelType.CHANNEL));
        assertThat(channel.getRegion(), is("ALL"));
        assertThat(
                channel.getUri(),
                is("http://nitro.bbc.co.uk/services/bbc_radio_fourlw_233a_1700")
        );
        assertThat(channel.getMediumDescription(), is("description"));
        assertThat(channel.getMediaType(), is(MediaType.AUDIO));
        assertThat(channel.getTitle(), is("name"));
        assertThat(channel.getSource(), is(Publisher.BBC_NITRO));
        assertThat(channel.getBroadcaster(), is(Publisher.BBC));
    }

    @Test
    public void dvbLocatorsGetLowercased() throws GlycerinException {
        Service service = getBasicService();
        setIds(service);

        when(response.getResults()).thenReturn(ImmutableList.of(service));

        ImmutableList<ModelWithPayload<Channel>> services = channelAdapter.fetchServices();
        ModelWithPayload<Channel> channelWithPayload = services.stream()
                .filter(chan -> chan.getModel().getCanonicalUri() != null)
                .findFirst()
                .get();

        Channel channel = channelWithPayload.getModel();
        assertThat(
                channel.getUri(),
                is("http://nitro.bbc.co.uk/services/bbc_radio_fourlw_233a_1700")
        );
        assertTrue(channel.getAliases().stream()
                .anyMatch(alias -> TERRESTRIAL_LOCATOR.toLowerCase().equals(alias.getValue())));
    }

    private void setDateRange(Service service) {
        DateRange dateRange = new DateRange();

        dateRange.setEnd(XMLGregorianCalendarImpl.createDate(2020,1,1,1));
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
        id2.setValue(TERRESTRIAL_LOCATOR);
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
        Brand.MasterBrand masterBrand = new Brand.MasterBrand();
        masterBrand.setMid("bbc_radio_fourlw");
        service.setMasterBrand(masterBrand);
        return service;
    }

    private MasterBrand setImages(MasterBrand masterBrand) {
        Brand.Images images = new Brand.Images();
        Brand.Images.Image image = new Brand.Images.Image();
        image.setTemplateUrl("uri");
        images.setImage(image);
        masterBrand.setImages(images);
        return masterBrand;
    }

    private void setSynopses(MasterBrand masterBrand) {
        MasterBrand.Synopses synopses = new MasterBrand.Synopses();
        synopses.setLong("long");
        synopses.setMedium("medium");
        synopses.setShort("short");
        masterBrand.setSynopses(synopses);
    }

}