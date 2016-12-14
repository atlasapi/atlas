package org.atlasapi.remotesite.rt;

import static org.atlasapi.persistence.logging.AdapterLogEntry.warnEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import nu.xom.Element;
import nu.xom.Elements;

import org.atlasapi.media.entity.Actor;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Certificate;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.CrewMember.Role;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Rating;
import org.atlasapi.media.entity.ReleaseDate;
import org.atlasapi.media.entity.ReleaseDate.ReleaseType;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Review;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.Subtitles;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.people.ItemsPeopleWriter;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.AdapterLogEntry;
import org.atlasapi.persistence.logging.AdapterLogEntry.Severity;
import org.atlasapi.remotesite.pa.PaCountryMap;
import org.atlasapi.remotesite.util.EnglishLanguageCodeMap;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;

import com.google.common.base.CharMatcher;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
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
    
    private final PaCountryMap countryMapper = new PaCountryMap();
    private final EnglishLanguageCodeMap languageMap = new EnglishLanguageCodeMap();
    
    private final Splitter csvSplitter = Splitter.on(",").omitEmptyStrings().trimResults();
    private final Splitter slashSplitter = Splitter.on("/").omitEmptyStrings().trimResults();

    public RtFilmProcessor(ContentResolver contentResolver, ContentWriter contentWriter, ItemsPeopleWriter peopleWriter, AdapterLog log) {
        this.contentResolver = contentResolver;
        this.contentWriter = contentWriter;
        this.peopleWriter = peopleWriter;
        this.log = log;
    }
    
    public void process(Element filmElement) {
        String id = filmElement.getFirstChildElement("film_reference_no").getValue();
        
        Film film;
        Identified existingFilm = contentResolver.findByCanonicalUris(ImmutableList.of(rtFilmUriFor(id))).getFirstValue().valueOrNull();
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
        if (hasValue(certificateElement) && MoreStrings.containsOnlyAsciiDigits(certificateElement.getValue())) {
            version.setRestriction(Restriction.from(Integer.parseInt(certificateElement.getValue())));
        }
        
        Element durationElement = filmElement.getFirstChildElement("running_time");
        if (hasValue(durationElement) && MoreStrings.containsOnlyAsciiDigits(durationElement.getValue())) {
            version.setDuration(Duration.standardMinutes(Long.parseLong(durationElement.getValue())));
        }
        
        Element threeD = filmElement.getFirstChildElement("three_D");
        if (hasValue(threeD)) {
            version.set3d("3D".equals(threeD.getValue()));
        }

        Element countriesElement = filmElement.getFirstChildElement("country_of_origin");
        if (hasValue(countriesElement)) {
            film.setCountriesOfOrigin(countryMapper.parseCountries(countriesElement.getValue()));
        }
        
        List<CrewMember> otherPublisherPeople = getOtherPublisherPeople(film);
        
        if (otherPublisherPeople.isEmpty()) {
            film.setPeople(ImmutableList.copyOf(Iterables.concat(getActors(filmElement.getFirstChildElement("cast")), getDirectors(filmElement.getFirstChildElement("direction")))));
        } else {
            film.setPeople(otherPublisherPeople);
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
        
        Element ukCinemaCertificate = filmElement.getFirstChildElement("UK_cinema_certificate_BBFC");
        if (hasValue(ukCinemaCertificate)) {
            film.setCertificates(certificate(ukCinemaCertificate));
        }

        ArrayList<Review> reviews = new ArrayList(2);
        Element normalReview = filmElement.getFirstChildElement("normal_review");
        if (hasValue(normalReview)) {
            reviews.add(new Review(Locale.ENGLISH, normalReview.getValue()));
        }

        Element shortReview = filmElement.getFirstChildElement("short_review");
        if (hasValue(shortReview)) {
            reviews.add(new Review(Locale.ENGLISH, shortReview.getValue()));
        }

        film.setReviews(reviews);

        Element starRating = filmElement.getFirstChildElement("rating");
        if (hasValue(starRating)) {
            int ratingValue = Character.getNumericValue(starRating.getValue().charAt(0));
            if (0 <= ratingValue) {
                Rating rating = new Rating(RT_RATING_SCHEME, ratingValue, film.getPublisher());
                film.setRatings(Collections.singletonList(rating));
            } else {
                log.record(warnEntry().withSource(getClass()).withDescription("Unable to parse %s rating scheme from '%s'", RT_RATING_SCHEME, starRating.getValue()));
            }
        }

        contentWriter.createOrUpdate(film);
        
        peopleWriter.createOrUpdatePeople(film);
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
        return new ReleaseDate(DateTimeFormat.forPattern("d/M/YYYY").parseDateTime(value).toLocalDate(), Countries.GB, ReleaseType.GENERAL);
    }

    public Iterable<String> extractOriginalLanguages(Element originalLanguages) {
        List<String> languageCodes = Lists.newArrayList();
        for (String originalLanguage : splitLanguages(originalLanguages.getValue())) {
            Optional<String> code = languageMap.codeForEnglishLanguageName(originalLanguage.toLowerCase());
            if (code.isPresent()) {
                languageCodes.add(code.get());
            } else {
                log.record(warnEntry().withSource(getClass()).withDescription("No language code for original language %s", originalLanguage));
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
        String csvLanguages = subtitlesElement.getValue().substring(0, subtitlesElement.getValue().indexOf('+'));
        List<Subtitles> subtitles = Lists.newArrayList();
        for (String subtitleLanguage : csvSplitter.split(csvLanguages)) {
            Optional<String> code = languageMap.codeForEnglishLanguageName(subtitleLanguage.toLowerCase());
            if (code.isPresent()) {
                subtitles.add(new Subtitles(code.get()));
            } else {
                log.record(warnEntry().withSource(getClass()).withDescription("No language code for subtitles %s", subtitleLanguage));
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

    private List<CrewMember> getOtherPublisherPeople(Film film) {
        Builder<CrewMember> builder = ImmutableList.builder();
        for (CrewMember crewMember : film.getPeople()) {
            if (crewMember.publisher() != Publisher.RADIO_TIMES) {
                builder.add(crewMember);
            }
        }
        return builder.build();
    }
    
    private List<Actor> getActors(Element castElement) {
        Elements actorElements = castElement.getChildElements("actor");
        
        List<Actor> actors = Lists.newArrayList();
        
        for (int i = 0; i < actorElements.size(); i++) {
            Element actorElement = actorElements.get(i);
            
            String role = actorElement.getFirstChildElement("role").getValue();
            
            actors.add(Actor.actorWithoutId(name(actorElement), role, Publisher.RADIO_TIMES));
        }
        
        return actors;
    }
    
    private List<CrewMember> getDirectors(Element directionElement) {
        Elements directorElements = directionElement.getChildElements("director");
        
        List<CrewMember> actors = Lists.newArrayList();
        
        for (int i = 0; i < directorElements.size(); i++) {
            Element directorElement = directorElements.get(i);
            
            String role = directorElement.getFirstChildElement("role").getValue();
            role = role.trim().replace(" ", "_").toLowerCase();
            
            String name = name(directorElement);
            
            if (name != null) {
                if (Role.fromPossibleKey(role).isNothing()) {
                    log.record(new AdapterLogEntry(Severity.WARN).withSource(getClass()).withDescription("Ignoring crew member with unrecognised role: " + role));
                } else {
                    actors.add(CrewMember.crewMemberWithoutId(name, role, Publisher.RADIO_TIMES));
                }
            }
        }
        
        return actors;
    }
    
    private String name(Element personElement) {

        Element forename = personElement.getFirstChildElement("forename");
        Element surname = personElement.getFirstChildElement("surname");

        if (forename == null && surname == null) {
            log.record(new AdapterLogEntry(Severity.WARN).withDescription("Person found with no name: " + personElement.toXML()).withSource(getClass()));
            return null;
        }

        if (forename != null && surname != null) {
            return forename.getValue() + " " + surname.getValue();
        } else {
            return forename != null ? forename.getValue() : surname.getValue();
        }
    }
}
