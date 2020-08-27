package org.atlasapi.input;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import org.atlasapi.media.channel.ChannelType;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.simple.Channel;
import org.atlasapi.media.entity.simple.Image;
import org.atlasapi.media.entity.simple.PublisherDetails;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.Date;
import java.time.Instant;
import java.util.stream.Collectors;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ChannelModelTransformerTest {

    private NumberToShortStringCodec v4Codec = mock(NumberToShortStringCodec.class);
    private ImageModelTranslator imageTranslator = mock(ImageModelTranslator.class);
    private ChannelModelTransformer channelTransformer;

    @Before
    public void setUp() throws Exception {
        this.channelTransformer = ChannelModelTransformer.create(
                v4Codec,
                imageTranslator
        );
    }

    @Test
    public void translatingChannelSimpleModelToComplexModel() {
        Channel simple = new Channel();
        simple.setUri("http://test.channel");
        simple.setBroadcaster(new PublisherDetails("hulu.com"));
        simple.setPublisherDetails(new PublisherDetails("hulu.com"));
        simple.setRegion("region");
        simple.setRegional(false);
        simple.setAdult(false);
        simple.setAdvertisedFrom(Date.from(Instant.parse("2016-04-04T00:00:00Z")));
        simple.setGenres(ImmutableList.of("action"));
        simple.setStartDate(LocalDate.fromDateFields(Date.from(Instant.parse("2016-04-04T00:00:00Z"))));
        simple.setEndDate(LocalDate.fromDateFields(Date.from(Instant.parse("2016-04-04T01:00:00Z"))));
        simple.setHighDefinition(true);
        Image image = new Image("http://image.com");
        Image image2 = new Image("http://another-image.com");
        org.atlasapi.media.entity.Image complexImage = new org.atlasapi.media.entity.Image("http://image.com");
        org.atlasapi.media.entity.Image complexImage2 = new org.atlasapi.media.entity.Image("http://another-image.com");
        when(imageTranslator.transform(image)).thenReturn(complexImage);
        when(imageTranslator.transform(image2)).thenReturn(complexImage2);
        simple.setImages(ImmutableSet.of(image, image2));
        simple.setTargetRegions(ImmutableSet.of("en", "au"));
        simple.setTitle("The Channel");
        simple.setType("video");
        simple.setMediaType("video");
        simple.setChannelType("channel");

        org.atlasapi.media.channel.Channel complex = channelTransformer.transform(simple);

        assertThat(complex.getUri(), is("http://test.channel"));
        assertThat(complex.getBroadcaster(), is(Publisher.HULU));
        assertThat(complex.getSource(), is(Publisher.HULU));
        assertThat(complex.getRegion(), is("region"));
        assertThat(complex.getRegional(), is(false));
        assertThat(complex.getAdult(), is(false));
        assertThat(complex.getHighDefinition(), is(true));
        assertThat(
                complex.getImages().stream()
                        .map(org.atlasapi.media.entity.Image::getCanonicalUri)
                        .collect(Collectors.toSet()),
                is(ImmutableSet.of("http://image.com", "http://another-image.com"))
        );
        assertThat(complex.getTargetRegions().size(), is(2));
        assertThat(complex.getTargetRegions().stream().findFirst().get(), is("en"));
        assertThat(complex.getTitle(), is("The Channel"));
        assertThat(complex.getMediaType(), is(MediaType.VIDEO));
        assertThat(complex.getChannelType(), is(ChannelType.CHANNEL));
        assertTrue(complex.getAdvertiseFrom() != null);
        assertTrue(complex.getStartDate() != null);
        assertThat(complex.getGenres().size(), is(1));
        assertThat(complex.getKey(), is("http://test.channel"));
        assertThat(complex.getGenres().stream().findFirst().get(), is("action"));
    }

}