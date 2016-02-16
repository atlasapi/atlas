package org.atlasapi.remotesite.bbc.nitro.extract;

import static com.metabroadcast.atlas.glycerin.model.Brand.Contributions;

import java.math.BigInteger;
import java.util.List;
import java.util.Set;

import javax.xml.datatype.XMLGregorianCalendar;

import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.ReleaseDate;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.bbc.BbcFeeds;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.atlas.glycerin.model.AncestorTitles;
import com.metabroadcast.atlas.glycerin.model.Brand.MasterBrand;
import com.metabroadcast.atlas.glycerin.model.Episode;
import com.metabroadcast.atlas.glycerin.model.Format;
import com.metabroadcast.atlas.glycerin.model.GenreGroup;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.atlas.glycerin.model.Synopses;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.time.Clock;

/**
 * <p>
 * A {@link BaseNitroItemExtractor} for extracting {@link Item}s from
 * {@link Episode} sources.
 * </p>
 * <p>
 * <p>
 * Creates and {@link Item} or {@link org.atlasapi.media.entity.Episode Atlas
 * Episode} and sets the parent and episode number fields as necessary.
 * </p>
 *
 * @see BaseNitroItemExtractor
 * @see NitroContentExtractor
 */
public final class NitroEpisodeExtractor extends BaseNitroItemExtractor<Episode, Item> {

    private static final String FILM_FORMAT_ID = "PT007";
    private static final Predicate<Format> IS_FILM_FORMAT = new Predicate<Format>() {

        @Override
        public boolean apply(Format input) {
            return FILM_FORMAT_ID.equals(input.getFormatId());
        }
    };


    private final ContentExtractor<List<GenreGroup>, Set<String>> genresExtractor = new NitroGenresExtractor();

    private final NitroCrewMemberExtractor crewMemberExtractor = new NitroCrewMemberExtractor();
    private final NitroPersonExtractor personExtractor = new NitroPersonExtractor();
    private final QueuingPersonWriter personWriter;

    public NitroEpisodeExtractor(Clock clock, QueuingPersonWriter personWriter) {
        super(clock);
        this.personWriter = personWriter;
    }

    @Override
    protected Item createContent(NitroItemSource<Episode> source) {
        if (isEpisode(source.getProgramme())) {
            return new org.atlasapi.media.entity.Episode();
        }

        if (isFilmFormat(source.getProgramme())) {
            return new Film();
        }

        return new Item();
    }

    private boolean isFilmFormat(Episode episode) {
        if (episode.getProgrammeFormats() == null) {
            return false;
        }

        return Iterables.any(episode.getProgrammeFormats().getFormat(), IS_FILM_FORMAT);
    }

    private boolean isEpisode(Episode episode) {
        return episode.getEpisodeOf() != null;
    }

    @Override
    protected String extractPid(NitroItemSource<Episode> source) {
        return source.getProgramme().getPid();
    }

    @Override
    protected String extractTitle(NitroItemSource<Episode> source) {
        return source.getProgramme().getTitle();
    }

    @Override
    protected Synopses extractSynopses(NitroItemSource<Episode> source) {
        return source.getProgramme().getSynopses();
    }

    @Override
    protected Contributions extractContributions(NitroItemSource<Episode> source) {
        return source.getProgramme().getContributions();
    }

    @Override
    protected com.metabroadcast.atlas.glycerin.model.Brand.Images.Image extractImage(
            NitroItemSource<Episode> source) {
        if (source.getProgramme().getImages() == null) {
            return null;
        }
        return source.getProgramme().getImages().getImage();
    }

    protected XMLGregorianCalendar extractReleaseDate(NitroItemSource<Episode> source) {
        return source.getProgramme().getReleaseDate();
    }

    @Override
    protected void extractAdditionalItemFields(NitroItemSource<Episode> source, Item item,
            DateTime now) {
        Episode episode = source.getProgramme();
        if (item.getTitle() == null) {
            item.setTitle(episode.getPresentationTitle());
        }
        if (hasMoreThanOneSeriesAncestor(episode)) {
            item.setTitle(compileTitleForSeriesSeriesEpisode(episode));
        }
        if (episode.getEpisodeOf() != null) {
            org.atlasapi.media.entity.Episode episodeContent = (org.atlasapi.media.entity.Episode) item;
            BigInteger position = episode.getEpisodeOf().getPosition();
            if (position != null) {
                episodeContent.setEpisodeNumber(position.intValue());
            }
            episodeContent.setSeriesRef(getSeriesRef(episode));
        }
        item.setParentRef(getBrandRef(episode));
        if(episode.getGenreGroupings() != null) {
            item.setGenres(genresExtractor.extract(episode.getGenreGroupings().getGenreGroup()));
        }
        if (episode.getReleaseDate() != null) {
            setReleaseDate(item, source);
        }
        writeAndSetPeople(item, source);
    }

    private void writeAndSetPeople(Item item, NitroItemSource<Episode> source) {
        Contributions contributions = source.getProgramme().getContributions();

        if (contributions != null) {
            ImmutableList.Builder<CrewMember> crewMembers = ImmutableList.builder();
            for (Contributions.Contribution contribution : contributions.getContributionsMixinContribution()) {
                Optional<CrewMember> crewMember = crewMemberExtractor.extract(contribution);

                if (crewMember.isPresent()) {
                    crewMembers.add(crewMember.get());
                    Optional<Person> person = personExtractor.extract(contribution);

                    if (person.isPresent()) {
                        personWriter.addItemToPerson(person.get(), item);
                    }
                }
            }

            item.setPeople(crewMembers.build());
        }
    }

    private boolean hasMoreThanOneSeriesAncestor(Episode episode) {
        AncestorTitles titles = episode.getAncestorTitles();
        return titles != null && titles.getSeries().size() > 1;
    }

    private void setReleaseDate(Item item, NitroItemSource<Episode> source) {
        XMLGregorianCalendar date = extractReleaseDate(source);
        LocalDate localDate = new LocalDate(date.getYear(), date.getMonth(), date.getDay());
        ReleaseDate releaseDate = new ReleaseDate(localDate,
                Countries.GB,
                ReleaseDate.ReleaseType.FIRST_BROADCAST);
        item.setReleaseDates(Lists.newArrayList(releaseDate));
    }

    private String compileTitleForSeriesSeriesEpisode(Episode episode) {
        List<AncestorTitles.Series> series = episode.getAncestorTitles().getSeries();
        String ssTitle = Iterables.getLast(series).getTitle();
        String suffix = "";
        if (episode.getPresentationTitle() != null) {
            suffix = " - " + episode.getPresentationTitle();
        } else if (episode.getTitle() != null) {
            suffix = " - " + episode.getTitle();
        }
        return ssTitle + suffix;
    }

    private ParentRef getBrandRef(Episode episode) {
        ParentRef brandRef = null;
        if (isBrandEpisode(episode)) {
            brandRef = new ParentRef(BbcFeeds.nitroUriForPid(episode.getEpisodeOf().getPid()));
        } else if (isBrandSeriesEpisode(episode)) {
            brandRef = getRefFromBrandAncestor(episode);
        } else if (isTopLevelSeriesEpisode(episode)) {
            AncestorTitles.Series topSeries = episode.getAncestorTitles().getSeries().get(0);
            brandRef = new ParentRef(BbcFeeds.nitroUriForPid(topSeries.getPid()));
        }
        return brandRef;
    }

    private ParentRef getRefFromBrandAncestor(Episode episode) {
        AncestorTitles.Brand brandAncestor = episode.getAncestorTitles().getBrand();
        return new ParentRef(BbcFeeds.nitroUriForPid(brandAncestor.getPid()));
    }

    private ParentRef getSeriesRef(Episode episode) {
        ParentRef seriesRef = null;
        if (isBrandSeriesEpisode(episode) || isTopLevelSeriesEpisode(episode)) {
            AncestorTitles.Series topSeries = episode.getAncestorTitles().getSeries().get(0);
            seriesRef = new ParentRef(BbcFeeds.nitroUriForPid(topSeries.getPid()));
        }
        return seriesRef;
    }

    private boolean isBrandEpisode(Episode episode) {
        PidReference episodeOf = episode.getEpisodeOf();
        return episodeOf != null
                && "brand".equals(episodeOf.getResultType());
    }

    private boolean isBrandSeriesEpisode(Episode episode) {
        PidReference episodeOf = episode.getEpisodeOf();
        return episodeOf != null
                && "series".equals(episodeOf.getResultType())
                && hasBrandAncestor(episode);
    }

    private boolean hasBrandAncestor(Episode episode) {
        return episode.getAncestorTitles() != null
                && episode.getAncestorTitles().getBrand() != null;
    }

    private boolean isTopLevelSeriesEpisode(Episode episode) {
        PidReference episodeOf = episode.getEpisodeOf();
        return episodeOf != null
                && "series".equals(episodeOf.getResultType())
                && !hasBrandAncestor(episode);
    }

    @Override
    protected String extractMediaType(NitroItemSource<Episode> source) {
        return source.getProgramme().getMediaType();
    }

    @Override
    protected MasterBrand extractMasterBrand(NitroItemSource<Episode> source) {
        return source.getProgramme().getMasterBrand();
    }

}
