package org.atlasapi.remotesite.pa.archives;

import java.util.List;

import org.atlasapi.remotesite.pa.listings.bindings.Billings;
import org.atlasapi.remotesite.pa.listings.bindings.Category;
import org.atlasapi.remotesite.pa.listings.bindings.Actor;
import org.atlasapi.remotesite.pa.listings.bindings.CastMember;
import org.atlasapi.remotesite.pa.listings.bindings.Link;
import org.atlasapi.remotesite.pa.listings.bindings.Links;
import org.atlasapi.remotesite.pa.listings.bindings.Person;
import org.atlasapi.remotesite.pa.listings.bindings.RtCategory;
import org.atlasapi.remotesite.pa.listings.bindings.Season;
import org.atlasapi.remotesite.pa.listings.bindings.StaffMember;
import org.atlasapi.remotesite.pa.listings.bindings.Billing;
import org.atlasapi.remotesite.pa.listings.bindings.Attr;
import org.atlasapi.remotesite.pa.listings.bindings.ProgData;
import org.atlasapi.remotesite.pa.listings.bindings.Warning;

public class PaDataToUpdatesTransformer {

    public ProgData transformToListingProgdata(
            org.atlasapi.remotesite.pa.archives.bindings.ProgData archive) {
        ProgData listing = new ProgData();

        Attr listingAttr = transformAttributes(archive.getAttr());
        listing.setAttr(listingAttr);

        if (archive.getBillings() != null) {
            transformAndSetBillings(archive.getBillings(), listing);
        }

        if (archive.getStaffMember() != null) {
            transformAndSetStaffMembers(archive.getStaffMember(), listing);
        }

        if (archive.getCastMember() != null) {
            transformAndSetCastMembers(archive.getCastMember(),listing);
        }

        if (archive.getCategory() != null) {
            transformAndSetCategories(archive.getCategory(), listing);
        }

        if (archive.getLinks() != null) {
            transformAndSetLinks(archive.getLinks(), listing);
        }

        listing.setCertificate(archive.getCertificate());
        listing.setColour(archive.getColour());
        listing.setCountry(archive.getCountry());
        listing.setDubbing(archive.getDubbing());
        listing.setEpisodeNumber(archive.getEpisodeNumber());
        listing.setEpisodeTitle(archive.getEpisodeTitle());
        listing.setEpisodeTotal(archive.getEpisodeTotal());
        listing.setFilmYear(archive.getFilmYear());
        listing.setGeneric(archive.getGeneric());
        listing.setGenre(archive.getGenre());
        listing.setGroup(archive.getGroup());
        listing.setProgId(archive.getProgId());
        listing.setProgrammeVersion(archive.getProgrammeVersion());
        listing.setRoviDescription(archive.getRoviDescription());
        listing.setRoviGenre(archive.getRoviGenre());
        listing.setRtFilmnumber(archive.getRtFilmnumber());
        listing.setSubtitles(archive.getSubtitles());
        listing.setSeriesId(archive.getSeriesId());
        listing.setSeriesNumber(archive.getSeriesNumber());
        listing.setSeriesSummary(archive.getSeriesSummary());
        listing.setSeriesSynopsis(archive.getSeriesSynopsis());
        listing.setSeriesVersion(archive.getSeriesVersion());
        listing.setShowingId(archive.getShowingId());
        listing.setStarRating(archive.getStarRating());
        listing.setTitle(archive.getTitle());

        if (archive.getWarning() != null) {
            Warning warning = new Warning();
            warning.setType(archive.getWarning().getType());
            warning.setvalue(archive.getWarning().getvalue());
            listing.setWarning(warning);
        }

        if (archive.getSeason() != null) {
            Season season = new Season();
            season.setId(archive.getSeason().getId());
            season.setNumber(archive.getSeason().getNumber());
            season.setSeasonSummary(archive.getSeason().getSeasonSummary());
            season.setSeasonTitle(archive.getSeason().getSeasonTitle());
            listing.setSeason(season);
        }

        if (archive.getRtCategory() != null) {
            RtCategory rtCategory = new RtCategory();
            rtCategory.setMaincat(archive.getRtCategory().getMaincat());
            rtCategory.setSubcat(archive.getRtCategory().getSubcat());
            listing.setRtCategory(rtCategory);
        }

        return listing;
    }

    private void transformAndSetLinks(org.atlasapi.remotesite.pa.archives.bindings.Links links, ProgData listing) {
        Links listingLinks = new Links();
        for (org.atlasapi.remotesite.pa.archives.bindings.Link link : links.getLink()) {
            Link listingLink = new Link();
            listingLink.setType(link.getType());
            listingLink.setvalue(link.getvalue());
            listingLinks.getLink().add(listingLink);
        }
        listing.setLinks(listingLinks);
    }

    private void transformAndSetCategories(List<org.atlasapi.remotesite.pa.archives.bindings.Category> categories, ProgData listing) {

        for (org.atlasapi.remotesite.pa.archives.bindings.Category category : categories) {
            Category listingCategory = new Category();
            listingCategory.setCategoryCode(category.getCategoryCode());
            listingCategory.setCategoryName(category.getCategoryName());
            listing.getCategory().add(listingCategory);
        }

    }

    private void transformAndSetCastMembers(List<org.atlasapi.remotesite.pa.archives.bindings.CastMember> castMember, ProgData listing) {

        for (org.atlasapi.remotesite.pa.archives.bindings.CastMember member : castMember) {
            CastMember listingMember = new CastMember();
            Actor actor = new Actor();
            actor.setPersonId(member.getActor().getPersonId());
            actor.setvalue(member.getActor().getvalue());
            listingMember.setCharacter(member.getCharacter());
            listingMember.setActor(actor);
            listing.getCastMember().add(listingMember);
        }

    }

    private void transformAndSetStaffMembers(List<org.atlasapi.remotesite.pa.archives.bindings.StaffMember> staffMembers, ProgData listing) {

        for (org.atlasapi.remotesite.pa.archives.bindings.StaffMember member : staffMembers) {
            StaffMember listingMember = new StaffMember();
            Person person = new Person();
            person.setPersonId(member.getPerson().getPersonId());
            person.setvalue(member.getPerson().getvalue());
            listingMember.setPerson(person);
            listingMember.setRole(member.getRole());
            listing.getStaffMember().add(listingMember);
        }

    }

    private Attr transformAttributes(org.atlasapi.remotesite.pa.archives.bindings.Attr archive) {

        Attr listing = new Attr();
        listing.setAsLive(archive.getAsLive());
        listing.setAudioDes(archive.getAudioDes());
        listing.setBw(archive.getBw());
        listing.setChoice(archive.getChoice());
        listing.setFamilychoice(archive.getFamilychoice());
        listing.setFilm(archive.getFilm());
        listing.setFollowOn(archive.getFollowOn());
        listing.setHd(archive.getHd());
        listing.setInteractive(archive.getInteractive());
        listing.setInVision(archive.getInVision());
        listing.setLastInSeries(archive.getLastInSeries());
        listing.setLive(archive.getLive());
        listing.setNewEpisode(archive.getNewEpisode());
        listing.setNewSeries(archive.getNewSeries());
        listing.setPremiere(archive.getPremiere());
        listing.setRepeat(archive.getRepeat());
        listing.setSignLang(archive.getSignLang());
        listing.setSpecial(archive.getSpecial());
        listing.setStereo(archive.getStereo());
        listing.setSubtitles(archive.getSubtitles());
        listing.setSurround(archive.getSurround());
        listing.setThreeD(archive.getThreeD());
        listing.setTvMovie(archive.getTvMovie());
        listing.setWebChoice(archive.getWebChoice());
        listing.setWidescreen(archive.getWidescreen());

        return listing;
    }

    private void transformAndSetBillings(org.atlasapi.remotesite.pa.archives.bindings.Billings billings, ProgData progdata) {
        if (billings.getBilling() != null) {

            Billings listingBillings = new Billings();
            for (org.atlasapi.remotesite.pa.archives.bindings.Billing billing : billings.getBilling()) {
                Billing listingBilling = new Billing();
                listingBilling.setReviewerInitials(billing.getReviewerInitials());
                listingBilling.setReviewerName(billing.getReviewerName());
                listingBilling.setType(billing.getType());
                listingBilling.setvalue(billing.getvalue());
                listingBillings.getBilling().add(listingBilling);
            }

            progdata.setBillings(listingBillings);
        }

    }

}
