package org.atlasapi.remotesite.barb.channels;

import java.util.Set;

import org.atlasapi.input.ChannelModelTransformer;
import org.atlasapi.input.ImageModelTransformer;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Alias;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class BarbStringToChannelTransformerTest {

    private ChannelModelTransformer modelTransformer;
    private BarbStringToChannelTransformer barbTransformer;

    @Before
    public void setUp() throws Exception {

        modelTransformer = ChannelModelTransformer.create(
                SubstitutionTableNumberCodec.lowerCaseOnly(),
                mock(ImageModelTransformer.class)
        );
        barbTransformer = BarbStringToChannelTransformer.create(modelTransformer);
    }

    @Test
    public void transformsSingleChannel() throws Exception {
        String channelString = "1234, Test channel v1";

        Channel c = barbTransformer.transform(channelString);

        assertThat(c.getAliases().size(), is(2));
        Set<Alias> aliases = c.getAliases();
        assertTrue(aliases.contains(new Alias("gb:barb:stationCode", "1234")));
        assertTrue(aliases.contains(new Alias("uri", c.getUri())));
        assertThat(c.getTitle(), is("Test channel v1"));

    }

    @Test
    public void transformsMultipleChannels() throws Exception {
        String channelStrings = " 1111 , Test 1 |2222,Test2|3333, Test 3";

        String[] splitChannels = channelStrings.split("\\|");
        assertThat(splitChannels.length, is(3));

        Channel[] channels = new Channel[3];

        for (int i = 0; i < splitChannels.length; i++) {
            channels[i] = barbTransformer.transform(splitChannels[i]);
        }

        assertThat(channels[0].getAliases().iterator().next().getValue(), is("1111"));
        assertThat(channels[0].getTitle(), is("Test 1"));

        assertThat(channels[1].getAliases().iterator().next().getValue(), is("2222"));
        assertThat(channels[1].getTitle(), is("Test2"));

        assertThat(channels[2].getAliases().iterator().next().getValue(), is("3333"));
        assertThat(channels[2].getTitle(), is("Test 3"));
    }

}