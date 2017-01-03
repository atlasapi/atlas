package org.atlasapi.remotesite.rt;

import static org.atlasapi.persistence.logging.AdapterLogEntry.warnEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import nu.xom.Element;
import nu.xom.Elements;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Certificate;
import org.atlasapi.media.entity.Distribution;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Language;
import org.atlasapi.media.entity.LocalizedTitle;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Rating;
import org.atlasapi.media.entity.ReleaseDate;
import org.atlasapi.media.entity.ReleaseDate.ReleaseType;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.Subtitles;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.people.ItemsPeopleWriter;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.remotesite.pa.PaCountryMap;
import org.atlasapi.remotesite.rt.extractors.RtPeopleExtractor;
import org.atlasapi.remotesite.rt.extractors.RtReviewExtractor;
import org.atlasapi.remotesite.util.EnglishLanguageCodeMap;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.text.MoreStrings;

public class RtFilmProcessor {
    
    private static final String RT_FILM_URI_BASE = "http://radiotimes.com/films/";
    private static final String RT_FILM_ALIAS = "rt:filmid";
    private static final String RT_RATING_SCHEME = "5STAR";
    
    private final ContentResolver contentResolver;
    private final ContentWriter contentWriter;
    private final ItemsPeopleWriter peopleWriter;
    private final AdapterLog log;
    private final QueuingPersonWriter queuingPersonWriter;
    
    private final PaCountryMap countryMapper = new PaCountryMap();
    private final EnglishLanguageCodeMap languageMap = new EnglishLanguageCodeMap();
    
    private final Splitter csvSplitter = Splitter.on(",").omitEmptyStrings().trimResults();
    private final Splitter slashSplitter = Splitter.on("/").omitEmptyStrings().trimResults();

    public RtFilmProcessor(
            ContentResolver contentResolver,
            ContentWriter contentWriter,
            ItemsPeopleWriter peopleWriter,
            AdapterLog log,
            QueuingPersonWriter queuingPersonWriter
    ) {
        this.contentResolver = contentResolver;
        this.contentWriter = contentWriter;
        this.peopleWriter = peopleWriter;
        this.log = log;
        this.queuingPersonWriter = queuingPersonWriter;
    }
    
    public void process(Element filmElement) {
        String id = filmElement.getFirstChildElement("film_reference_no").getValue();
        
        Film film;
        Identified existingFilm = contentResolver.findByCanonicalUris(
                ImmutableList.of(rtFilmUriFor(id))
        ).getFirstValue().valueOrNull();

        if (existingFilm instanceof Film) {
            film = (Film) existingFilm;
        } else if (existingFilm instanceof Item) {
            film = new Film();
            Item.copyTo((Item) existingFilm, film);
        } else {
            film = new Film(rtFilmUriFor(id), rtCurieFor(id), Publisher.RADIO_TIMES);
        }

        Element imdbElem = filmElement.getFirstChildElement("imdb_ref");
        if (imdbElem != null) {
            // TODO new alias
            film.addAliasUrl(normalize(imdbElem.getValue()));
        }
        film.setAliases(ImmutableSet.of(new Alias(RT_FILM_ALIAS, id)));

        film.setSpecialization(Specialization.FILM);
        film.setTitle(filmElement.getFirstChildElement("title").getValue());
        String year = filmElement.getFirstChildElement("year").getValue();
        if (!Strings.isNullOrEmpty(year) && MoreStrings.containsOnlyAsciiDigits(year)) {
            film.setYear(Integer.parseInt(year));
        }

        Version version = Iterables.getFirst(film.getVersions(), new Version());
        
        // Due to a bug we were creating multiple versions; setting explicitly here to a single
        // version to remove the erroneous ones;
        film.setVersions(ImmutableSet.of(version));
        
        version.setProvider(Publisher.RADIO_TIMES);
        Element certificateElement = filmElement.getFirstChildElement("certificate");
        if (hasValue(certificateElement) && MoreStrings.containsOnlyAsciiDigits(
                certificateElement.getValue()
        )) {
            version.setRestriction(Restriction.from(
                    Integer.parseInt(certificateElement.getValue())
            ));
        }
        
        Element durationElement = filmElement.getFirstChildElement("running_time");
        if (hasValue(durationElement) && MoreStrings.containsOnlyAsciiDigits(
                durationElement.getValue()
        )) {
            version.setDuration(Duration.standardMinutes(
                    Long.parseLong(durationElement.getValue())
            ));
        }
        
        Element threeD = filmElement.getFirstChildElement("three_D");
        if (hasValue(threeD)) {
            version.set3d("3D".equals(threeD.getValue()));
        }

        Element countriesElement = filmElement.getFirstChildElement("country_of_origin");
        if (hasValue(countriesElement)) {
            film.setCountriesOfOrigin(countryMapper.parseCountries(countriesElement.getValue()));
        }
        
        Element subtitlesElement = filmElement.getFirstChildElement("subtitles");
        if (hasValue(subtitlesElement)) {
            film.setSubtitles(extractSubtitles(subtitlesElement));
        }
        
        Element originalLanguages = filmElement.getFirstChildElement("original_language");
        if (hasValue(originalLanguages)) {
            film.setLanguages(extractOriginalLanguages(originalLanguages));
        }
        
        Element releaseDate = filmElement.getFirstChildElement("UK_release_date");
        if (hasValue(releaseDate)) {
            film.setReleaseDates(ImmutableList.of(ukReleaseDate(releaseDate.getValue())));
        }
        
        Element colour = filmElement.getFirstChildElement("colour");
        if (hasValue(colour)) {
            if ("Colour".equals(colour.getValue())) {
                film.setBlackAndWhite(false);
            } else if ("BW".equals(colour.getValue())) {
                film.setBlackAndWhite(true);
            }
        }
        
        Element ukCinemaCertificate = filmElement.getFirstChildElement(
                "UK_cinema_certificate_BBFC"
        );
        if (hasValue(ukCinemaCertificate)) {
            film.setCertificates(certificate(ukCinemaCertificate));
        }

        Element starRating = filmElement.getFirstChildElement("rating");
        if (hasValue(starRating)) {
            int ratingValue = Character.getNumericValue(starRating.getValue().charAt(0));
            if (0 <= ratingValue) {
                Rating rating = new Rating(RT_RATING_SCHEME, ratingValue, film.getPublisher());
                film.setRatings(Collections.singletonList(rating));
            } else {
                log.record(warnEntry().withSource(getClass()).withDescription(
                        "Unable to parse %s rating scheme from '%s'",
                        RT_RATING_SCHEME,
                        starRating.getValue()
                ));
            }
        }

        RtReviewExtractor rtReviewExtractor = RtReviewExtractor.create();
        film.setReviews(rtReviewExtractor.extractReviews(filmElement));

        makeLanguage(filmElement).ifPresent(film::setLanguage);

        film.setDistributions(makeDistributions(filmElement));

        RtPeopleExtractor rtPeopleExtractor = RtPeopleExtractor.create(filmElement, film, log);
        rtPeopleExtractor.process();
        List<Person> listOfPeople = rtPeopleExtractor.getPeople();

        Element alternativeTitles = filmElement.getFirstChildElement("alternative_title");
        if (hasValue(alternativeTitles)) {
            film.setLocalizedTitles(findAlternativeTitles(alternativeTitles));
        }

        Element synopsis = filmElement.getFirstChildElement("synopsis");
        if (hasValue(synopsis)) {
            film.setLongDescription(synopsis.getValue());
        }

        Element finalScreenplaySource = filmElement.getFirstChildElement("final_screenplay_source");
        if (hasValue(finalScreenplaySource)) {
            film.setMediumDescription(finalScreenplaySource.getValue());
        }

        Element oneLineSynopsis = filmElement.getFirstChildElement("one_line_synopsis");
        if (hasValue(oneLineSynopsis)) {
            film.setShortDescription(oneLineSynopsis.getValue());
        }

        Element warnings = filmElement.getFirstChildElement("warnings");

        if (hasValue(warnings)) {
            Restriction restriction = new Restriction();
            restriction.setMessage(warnings.getValue());
            version.setRestriction(restriction);
        }


        Element genres = filmElement.getFirstChildElement("genres");
        if (hasValue(genres)) {
            film.setGenres(Lists.newArrayList(
                    String.format("http://film.rt.com/genres/%s", genres.getValue())
            ));
        }

        Element rating = filmElement.getFirstChildElement("rating");
        if (hasValue(rating)) {
            film.setRatings(Lists.newArrayList(new Rating(
                    "Stars",
                    Float.parseFloat(rating.toString().split(" ")[0]),
                    Publisher.RADIO_TIMES
            )));
        }

        for (Person person : listOfPeople) {
            queuingPersonWriter.addItemToPerson(person, film);
        }

        contentWriter.createOrUpdate(film);
        
        peopleWriter.createOrUpdatePeople(film);
    }

    private DateTime parseReleaseDate(String dateString) {
        String[] dateStringArray = dateString.split("/");
        DateTime dateTime = new DateTime();
        dateTime.withDayOfMonth(Integer.parseInt(dateStringArray[0]));
        dateTime.withMonthOfYear(Integer.parseInt(dateStringArray[1]));
        dateTime.withYear(Integer.parseInt(dateStringArray[2]));

        return dateTime;
    }

    private List<Distribution> makeDistributions(Element filmElement) {
        List<Distribution> distributions = new ArrayList<>();

        Element dvdDistribution = filmElement.getFirstChildElement("available_on_DVD");
        Element dvdDistributor = filmElement.getFirstChildElement("DVD_distributor");
        Element ukReleaseDate = filmElement.getFirstChildElement("UK_release_date");

        if (hasValue(dvdDistribution)) {
            if (dvdDistribution.getValue().equals("Yes")) {
                distributions.add(Distribution.builder()
                        .withFormat("DVD")
                        .withDistributor(
                                hasValue(dvdDistributor) ?
                                dvdDistributor.getValue() :
                                null
                        )
                        .withReleaseDate(
                                hasValue(ukReleaseDate) ?
                                parseReleaseDate(ukReleaseDate.getValue()) :
                                null
                        )
                        .build()
                );
            }
        }

        Element bluRayDistribution = filmElement.getFirstChildElement("available_on_blu_ray");

        if (hasValue(bluRayDistribution)) {
            if (bluRayDistribution.getValue().equals("Yes")) {
                distributions.add(Distribution.builder()
                        .withFormat("BluRay")
                        .withDistributor("")
                        .withReleaseDate(null)
                        .build()
                );
            }
        }

        return distributions;
    }

    private Optional<Language> makeLanguage(Element filmElement) {
        Element dubbing = filmElement.getFirstChildElement("dubbing");
        if (hasValue(dubbing)) {
            return Optional.of(Language.builder()
                    .withDubbing(dubbing.getValue())
                    .withCode("")
                    .withDisplay("")
                    .build());
        } else {
            return Optional.empty();
        }
    }

    private List<LocalizedTitle> findAlternativeTitles(Element titles) {
        List<String> possibleTitles = Lists.newArrayList(
                "alt_title",
                "rt_style_title",
                "vers_title",
                "english_translation_title",
                "alphabetical_title"
        );

        List<LocalizedTitle> localizedTitles = Lists.newArrayList();

        for (String title : possibleTitles) {
            localizedTitles.addAll(makeAlternativeTitles(titles, title));
        }

        return localizedTitles;
    }

    private List<LocalizedTitle> makeAlternativeTitles(Element titles, String titleName) {
        List<LocalizedTitle> localizedTitles = Lists.newArrayList();

        Elements alternativeTitle = titles.getChildElements(titleName);

        for (int i = 0; i < alternativeTitle.size(); i++) {
            Element altTitle = alternativeTitle.get(i);

            if (hasValue(altTitle)) {
                LocalizedTitle localizedTitle = new LocalizedTitle();

                localizedTitle.setTitle(altTitle.getValue());
                localizedTitle.setType(titleName);
                localizedTitle.setLocale(new Locale("uk"));

                localizedTitles.add(localizedTitle);
            }
        }

        return localizedTitles;
    }

    public String rtCurieFor(String id) {
        return "rt:f-"+id;
    }

    public String rtFilmUriFor(String id) {
        return RT_FILM_URI_BASE + id;
    }

    private Set<Certificate> certificate(Element ukCinemaCertificate) {
        return ImmutableSet.of(new Certificate(ukCinemaCertificate.getValue(), Countries.GB));
    }

    private ReleaseDate ukReleaseDate(String value) {
        return new ReleaseDate(DateTimeFormat.forPattern("d/M/YYYY")
                .parseDateTime(value).toLocalDate(), Countries.GB, ReleaseType.GENERAL);
    }

    public Iterable<String> extractOriginalLanguages(Element originalLanguages) {
        List<String> languageCodes = Lists.newArrayList();
        for (String originalLanguage : splitLanguages(originalLanguages.getValue())) {
            com.google.common.base.Optional<String> code =
                    languageMap.codeForEnglishLanguageName(originalLanguage.toLowerCase());
            if (code.isPresent()) {
                languageCodes.add(code.get());
            } else {
                log.record(warnEntry().withSource(getClass())
                        .withDescription(
                                "No language code for original language %s",
                                originalLanguage
                        ));
            }
        }
        return languageCodes;
    }

    public Iterable<String> splitLanguages(String originalLanguage) {
        Splitter splitter = originalLanguage.indexOf('/') < 0 ? csvSplitter : slashSplitter;
        return splitter.split(originalLanguage);
    }

    public Iterable<Subtitles> extractSubtitles(Element subtitlesElement) {
        //always ends in "+subtitles" so remove it. 
        String csvLanguages = subtitlesElement.getValue()
                .substring(0, subtitlesElement.getValue().indexOf('+'));
        List<Subtitles> subtitles = Lists.newArrayList();
        for (String subtitleLanguage : csvSplitter.split(csvLanguages)) {
            com.google.common.base.Optional<String> code =
                    languageMap.codeForEnglishLanguageName(subtitleLanguage.toLowerCase());
            if (code.isPresent()) {
                subtitles.add(new Subtitles(code.get()));
            } else {
                log.record(warnEntry().withSource(getClass())
                        .withDescription("No language code for subtitles %s", subtitleLanguage));
            }
        }
        return subtitles;
    }

    public boolean hasValue(Element subtitlesElement) {
        return subtitlesElement != null && !Strings.isNullOrEmpty(subtitlesElement.getValue());
    }
    
    private String normalize(String imdbRef) {
        String httpRef = imdbRef.replace("www.", "http://");
        return CharMatcher.is('/').trimTrailingFrom(httpRef);
    }
}
