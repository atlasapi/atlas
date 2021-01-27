package org.atlasapi.input;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.time.DateTimeZones;
import org.atlasapi.media.TransportSubType;
import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Provider;
import org.joda.time.DateTime;

import java.util.Currency;
import java.util.Date;

public class EncodingModelTransformer {

    private EncodingModelTransformer() {

    }

    public static EncodingModelTransformer create() {
        return new EncodingModelTransformer();
    }

    public Encoding transform(org.atlasapi.media.entity.simple.Location simple, DateTime now) {
        Encoding encoding = encodingFrom(simple, now);
        Location location = locationFrom(simple, now);
        Policy policy = policyFrom(simple, now);
        location.setPolicy(policy);
        encoding.setAvailableAt(ImmutableSet.of(location));
        return encoding;
    }

    private Encoding encodingFrom(org.atlasapi.media.entity.simple.Location inputLocation, DateTime now) {
        Encoding encoding = new Encoding();
        encoding.setLastUpdated(now);
        encoding.setAdvertisingDuration(inputLocation.getAdvertisingDuration());
        encoding.setAudioBitRate(inputLocation.getAudioBitRate());
        encoding.setAudioChannels(inputLocation.getAudioChannels());
        encoding.setAudioCoding(asMimeType(inputLocation.getAudioCoding()));
        encoding.setBitRate(inputLocation.getBitRate());
        encoding.setContainsAdvertising(inputLocation.getContainsAdvertising());
        encoding.setDataContainerFormat(asMimeType(inputLocation.getDataContainerFormat()));
        encoding.setDataSize(inputLocation.getDataSize());
        encoding.setDistributor(inputLocation.getDistributor());
        encoding.setHasDOG(inputLocation.getHasDOG());
        encoding.setSource(inputLocation.getSource());
        encoding.setVideoAspectRatio(inputLocation.getVideoAspectRatio());
        encoding.setVideoBitRate(inputLocation.getVideoBitRate());
        encoding.setVideoCoding(asMimeType(inputLocation.getVideoCoding()));
        encoding.setVideoFrameRate(inputLocation.getVideoFrameRate());
        encoding.setVideoHorizontalSize(inputLocation.getVideoHorizontalSize());
        encoding.setVideoProgressiveScan(inputLocation.getVideoProgressiveScan());
        encoding.setVideoVerticalSize(inputLocation.getVideoVerticalSize());
        encoding.setHighDefinition(inputLocation.getHighDefinition());
        return encoding;
    }

    private Location locationFrom(org.atlasapi.media.entity.simple.Location inputLocation, DateTime now) {
        Location location = new Location();
        location.setLastUpdated(now);
        location.setCanonicalUri(inputLocation.getCanonicalUri());
        location.setEmbedCode(inputLocation.getEmbedCode());
        location.setEmbedId(inputLocation.getEmbedId());
        location.setTransportIsLive(inputLocation.getTransportIsLive());
        location.setUri(inputLocation.getUri());
        if(inputLocation.getProvider() != null) {
            location.setProvider(providerFrom(inputLocation.getProvider()));
        }
        if (inputLocation.getTransportSubType() != null) {
            location.setTransportSubType(TransportSubType.fromString(inputLocation.getTransportSubType()));
        }
        if (inputLocation.getTransportType() != null) {
            location.setTransportType(TransportType.fromString(inputLocation.getTransportType()));
        }
        return location;
    }

    private Provider providerFrom(org.atlasapi.media.entity.simple.Provider simpleProvider) {
        return new Provider(simpleProvider.getName(), simpleProvider.getIconUrl());
    }

    private Policy policyFrom(org.atlasapi.media.entity.simple.Location inputLocation, DateTime now) {
        Policy policy = new Policy();
        policy.setLastUpdated(now);
        policy.setAvailabilityStart(asUtcDateTime(inputLocation.getAvailabilityStart()));
        policy.setAvailabilityEnd(asUtcDateTime(inputLocation.getAvailabilityEnd()));
        policy.setDrmPlayableFrom(asUtcDateTime(inputLocation.getDrmPlayableFrom()));
        policy.setPlatform(Policy.Platform.fromKey(inputLocation.getPlatform()));
        if (inputLocation.getCurrency() != null && inputLocation.getPrice() != null) {
            Currency currency = Currency.getInstance(inputLocation.getCurrency());
            policy.setPrice(new Price(currency, inputLocation.getPrice()));
        }
        if (inputLocation.getAvailableCountries() != null) {
            policy.setAvailableCountries(Countries.fromCodes(inputLocation.getAvailableCountries()));
        }
        policy.setRevenueContract(Policy.RevenueContract.fromKey(inputLocation.getRevenueContract()));
        return policy;
    }

    private MimeType asMimeType(String mimeType) {
        if (mimeType != null) {
            return MimeType.fromString(mimeType);
        }
        return null;
    }

    private DateTime asUtcDateTime(Date date) {
        if (date == null) {
            return null;
        }
        return new DateTime(date).withZone(DateTimeZones.UTC);
    }
}
