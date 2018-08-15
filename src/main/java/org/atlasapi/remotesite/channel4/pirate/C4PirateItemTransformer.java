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
import org.atlasapi.remotesite.channel4.pirate.model.Epg;
import org.atlasapi.remotesite.channel4.pirate.model.EpisodeSeriesBrand;
import org.atlasapi.remotesite.channel4.pirate.model.Synopses;

import javax.annotation.Nullable;

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

    private Brand createBrand(EditorialInformation info) {

        if (info.getBrand() == null) {
            return null;
        }

        String title = findTitle(info.getBrand(), info);

        if (Strings.isNullOrEmpty(title)) {
            return null;
        }

        Brand brand = new Brand(
                format(BRAND_URI, spaceToDash(title)),
                format("c4i:b-%s", spaceToDash(title)),
                Publisher.C4_INT
        );

        setCommonFields(findSynopses(info.getBrand(), info), brand);

        return brand;
    }

    private Series createSeries(EditorialInformation info) {

        if (info.getSeries() == null) {
            return null;
        }

        String title = findTitle(info.getSeries(), info);

        if (Strings.isNullOrEmpty(title)) {
            return null;
        }

        Series series = new Series(
                format(SERIES_URI, spaceToDash(title)),
                format("c4i:s-%s", info.getContractNumber()),
                Publisher.C4_INT
        );

        setCommonFields(findSynopses(info.getSeries(), info), series);

        series.setTitle(title);

        return series;
    }

    private Episode createEpisode(EditorialInformation info) {
        String title = findTitle(info.getEpisode(), info);

        if (Strings.isNullOrEmpty(title)) {
            return null;
        }

        Episode episode = new Episode(
                format(EPISODE_URI, spaceToDash(title)),
                format("c4i:e-%s/%s", info.getContractNumber(), pad0(info.getProgrammeNumber())),
                Publisher.C4_INT
        );

        setCommonFields(findSynopses(info.getEpisode(), info), episode);

        episode.setTitle(title);

        episode.setAliases(ImmutableSet.of(createAlias(info)));
        episode.setEpisodeNumber(Integer.parseInt(info.getProgrammeNumber()));

        return episode;
    }

    private void setCommonFields(Synopses synopses, Content content) {

        content.setShortDescription(synopses.getShortSynopsis());
        content.setMediumDescription(synopses.getMediumSynopsis());
        content.setLongDescription(synopses.getLongSynopsis());

        content.setGenres(ImmutableSet.of());
    }

    private Synopses fromDescription(String description) {
        return new Synopses(description, description, description);
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

    private String spaceToDash(String title) {
        return title.toLowerCase().replaceAll(" ", "-");
    }

    @Nullable
    private String findTitle(C4Com c4Com, Epg epg) {
        if (c4Com != null) {
            return c4Com.getTitle();
        } else if (epg != null) {
            return epg.getTitle();
        }

        return null;
    }

    @Nullable
    private Synopses findSynopses(C4Com c4Com, Epg epg) {
        if (c4Com != null) {
            if (c4Com.getSynopses() != null) {
                return c4Com.getSynopses();
            }
        }

        if (epg != null) {
            if (!Strings.isNullOrEmpty(epg.getDescription())) {
                return fromDescription(epg.getDescription());
            }
        }

        return null;
    }

    // Ugly but fault tolerant synopses finding. Order of search was specific
    @Nullable
    private Synopses findSynopses(org.atlasapi.remotesite.channel4.pirate.model.Brand brand, EditorialInformation info) {
        if (brand != null) {
            Synopses synopses = findSynopses(brand.getC4Com(), null);
            if (synopses != null) {
                return synopses;
            }
        }

        if (info.getSeries() != null) {
            Synopses synopses = findSynopses(info.getSeries().getC4Com(), info.getSeries().getEpg());
            if (synopses != null) {
                return synopses;
            }
        }

        if (info.getEpisode() != null) {
            Synopses synopses = findSynopses(info.getEpisode().getC4Com(), info.getEpisode().getEpg());
            if (synopses != null) {
                return synopses;
            }
        }

        return null;
    }

    @Nullable
    private Synopses findSynopses(org.atlasapi.remotesite.channel4.pirate.model.Series series, EditorialInformation info) {
        if (series != null) {
            Synopses synopses = findSynopses(series.getC4Com(), series.getEpg());
            if (synopses != null) {
                return synopses;
            }
        }

        if (info.getBrand() != null) {
            Synopses synopses = findSynopses(info.getBrand().getC4Com(), null);
            if (synopses != null) {
                return synopses;
            }
        }

        if (info.getEpisode() != null) {
            Synopses synopses = findSynopses(info.getEpisode().getC4Com(), info.getEpisode().getEpg());
            if (synopses != null) {
                return synopses;
            }
        }

        return null;
    }

    @Nullable
    private Synopses findSynopses(org.atlasapi.remotesite.channel4.pirate.model.Episode episode, EditorialInformation info) {
        if (episode != null) {
            Synopses synopses = findSynopses(episode.getC4Com(), episode.getEpg());
            if (synopses != null) {
                return synopses;
            }
        }

        if (info.getSeries() != null) {
            Synopses synopses = findSynopses(info.getSeries().getC4Com(), info.getSeries().getEpg());
            if (synopses != null) {
                return synopses;
            }
        }

        if (info.getBrand() != null) {
            Synopses synopses = findSynopses(info.getBrand().getC4Com(), null);
            if (synopses != null) {
                return synopses;
            }
        }

        return null;
    }

    // Ugly but fault tolerant title finding. Order of search was specific
    @Nullable
    private String findTitle(org.atlasapi.remotesite.channel4.pirate.model.Brand brand, EditorialInformation info) {
        if (brand != null) {
            String title = findTitle(brand.getC4Com(), null);
            if (!Strings.isNullOrEmpty(title)) {
                return title;
            }
        }

        if (info.getSeries() != null) {
            String title = findTitle(info.getSeries().getC4Com(), info.getSeries().getEpg());
            if (!Strings.isNullOrEmpty(title)) {
                return title;
            }
        }

        if (info.getEpisode() != null) {
            String title = findTitle(info.getEpisode().getC4Com(), info.getEpisode().getEpg());
            if (!Strings.isNullOrEmpty(title)) {
                return title;
            }
        }

        return null;
    }

    @Nullable
    private String findTitle(org.atlasapi.remotesite.channel4.pirate.model.Series series, EditorialInformation info) {

        if (series != null) {
            String title = findTitle(series.getC4Com(), series.getEpg());
            if (!Strings.isNullOrEmpty(title)) {
                return title;
            }
        }

        if (info.getBrand() != null) {
            String title = findTitle(info.getBrand().getC4Com(), null);
            if (!Strings.isNullOrEmpty(title)) {
                return title;
            }
        }

        if (info.getEpisode() != null) {
            String title = findTitle(info.getEpisode().getC4Com(), info.getEpisode().getEpg());
            if (!Strings.isNullOrEmpty(title)) {
                return title;
            }
        }

        return null;
    }

    @Nullable
    private String findTitle(org.atlasapi.remotesite.channel4.pirate.model.Episode episode, EditorialInformation info) {

        if (episode != null) {
            String title = findTitle(episode.getC4Com(), episode.getEpg());
            if (!Strings.isNullOrEmpty(title)) {
                return title;
            }
        }

        if (info.getSeries() != null) {
            String title = findTitle(info.getSeries().getC4Com(), info.getSeries().getEpg());
            if (!Strings.isNullOrEmpty(title)) {
                return title;
            }
        }

        if (info.getBrand() != null) {
            String title = findTitle(info.getBrand().getC4Com(), null);
            if (!Strings.isNullOrEmpty(title)) {
                return title;
            }
        }

        return null;
    }

}
