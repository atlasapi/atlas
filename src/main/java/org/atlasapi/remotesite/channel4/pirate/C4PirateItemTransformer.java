package org.atlasapi.remotesite.channel4.pirate;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.channel4.pirate.model.C4Com;
import org.atlasapi.remotesite.channel4.pirate.model.EditorialInformation;
import org.atlasapi.remotesite.channel4.pirate.model.EpisodeSeriesBrand;
import org.atlasapi.remotesite.channel4.pirate.model.Synopses;

import static java.lang.String.format;

public class C4PirateItemTransformer {

    private static final String C4_ALIAS_NAMESPACE = "gb:c4:episode:id";
    private static final String EPISODE_URI = Publisher.C4_INT.key() + "/episodes/%s";
    private static final String BRAND_URI = Publisher.C4_INT.key() + "/brands/%s";
    private static final String SERIES_URI = Publisher.C4_INT.key() + "/series/%s";

    private C4PirateItemTransformer() {

    }

    public static C4PirateItemTransformer create() {
        return new C4PirateItemTransformer();
    }

    public EpisodeSeriesBrand toEpisodeSeriesBrand(EditorialInformation info) {
        Episode episode = createEpisode(info);
        Series series = createSeries(info);
        Brand brand = createBrand(info);

        return new EpisodeSeriesBrand(episode, series, brand);
    }

    private Episode createEpisode(EditorialInformation info) {
        C4Com c4Com = info.getEpisode().getC4Com();

        Episode episode = new Episode(
                format(EPISODE_URI, c4Com.getTitle()),
                format("c4i:e-%s/%s", info.getContractNumber(), pad0(info.getProgrammeNumber())),
                Publisher.C4_INT
        );

        setCommonFields(c4Com, episode);

        episode.setAliases(ImmutableSet.of(createAlias(info)));
        episode.setEpisodeNumber(Integer.parseInt(info.getProgrammeNumber()));

        return episode;
    }

    private Brand createBrand(EditorialInformation info) {
        C4Com c4Com = info.getEpisode().getC4Com();

        Brand brand = new Brand(
                format(BRAND_URI, c4Com.getTitle()),
                format("c4i:b-%s", reformatBrandTitle(c4Com.getTitle())),
                Publisher.C4_INT
        );

        setCommonFields(c4Com, brand);

        return brand;
    }

    private Series createSeries(EditorialInformation info) {
        C4Com c4Com = info.getEpisode().getC4Com();

        Series series = new Series(
                format(SERIES_URI, c4Com.getTitle()),
                format("c4i:s-%s", info.getContractNumber()),
                Publisher.C4_INT
        );

        setCommonFields(c4Com, series);

        return series;
    }

    private void setCommonFields(C4Com c4Com, Content content) {
        content.setPublisher(Publisher.C4_INT);
        content.setTitle(c4Com.getTitle());

        Synopses synopses = c4Com.getSynopses();
        content.setShortDescription(synopses.getShortSynopsis());
        content.setMediumDescription(synopses.getMediumSynopsis());
        content.setLongDescription(synopses.getLongSynopsis());

        content.setGenres(ImmutableSet.of());
    }

    private Alias createAlias(EditorialInformation info) {
        String contractNo = info.getContractNumber();
        String programmeNo = info.getProgrammeNumber();

        return new Alias(
                C4_ALIAS_NAMESPACE,
                String.join("/", contractNo, pad0(programmeNo))
        );
    }

    private String pad0(String s) {
        return Strings.padStart(s, 3, '0');
    }

    private String reformatBrandTitle(String title) {
        return title.toLowerCase().replaceAll(" ", "-");
    }

}
