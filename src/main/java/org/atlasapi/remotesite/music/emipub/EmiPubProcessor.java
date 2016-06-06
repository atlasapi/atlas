package org.atlasapi.remotesite.music.emipub;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Song;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.logging.AdapterLog;

import com.metabroadcast.common.intl.Countries;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 */
public class EmiPubProcessor {

    private static final int NOT_FOUND = -1;
    //
    private static final Pattern SPLIT_PATTERN = Pattern.compile(":");
    private static final String EMI_PUB_WORKS_URI = "http://emimusicpub.com/works/";
    private static final String EMI_PUB_RIGHTS_URI = "http://emimusicpub.com/rights/";
    private static final String EMI_PUB_CURIE_PREFIX = "emipub:";

    public void process(File csvFile, AdapterLog log, ContentWriter contentWriter) throws Exception {
        BufferedReader csv = getReader(csvFile);

        try {
            String currentWork = csv.readLine();
            String currentCode = null;
            String currentId = null;
            String currentWriter = null;
            Map<String, Float> currentRights = new HashMap<String, Float>();
            Song currentSong = null;
            while (true) {
                Iterable<String> data = currentWork != null ? Splitter.on(SPLIT_PATTERN).trimResults().split(currentWork) : null;
                if (data != null) {
                    String nextCode = Iterables.get(data, 0);
                    if (!nextCode.equals(currentCode)) {
                        if (currentSong != null) {
                            makeSong(currentId, currentRights, currentSong, contentWriter);
                        }
                        currentRights.clear();
                        currentCode = nextCode;
                        currentId = Iterables.get(data, 1);
                        currentWriter = null;
                        currentSong = new Song(EMI_PUB_WORKS_URI + currentCode, EMI_PUB_CURIE_PREFIX + currentCode, Publisher.EMI_PUB);
                        currentSong.setTitle(Iterables.get(data, 2));
                    }
                    //
                    String nextWriter = Iterables.get(data, 4);
                    if (!nextWriter.equals(currentWriter)) {
                        currentWriter = nextWriter;
                        currentSong.addPerson(new CrewMember(null, null, Publisher.EMI_PUB).withName(currentWriter).withRole(CrewMember.Role.WRITER));
                    }
                    //
                    String right = Iterables.get(data, 9);
                    Float share = Float.parseFloat(Iterables.get(data, 11));
                    Float currentShareForRight = currentRights.getOrDefault(right, 0F);
                    currentRights.put(right, currentShareForRight + share);
                    currentWork = csv.readLine();
                } else {
                    makeSong(currentId, currentRights, currentSong, contentWriter);
                    break;
                }
            }
        } finally {
            csv.close();
        }
    }

    private void makeSong(String currentId, Map<String, Float> currentRights, Song currentSong, ContentWriter contentWriter) {
        Version version = new Version();
        Restriction restriction = new Restriction();
        Encoding encoding = new Encoding();
        Location location = new Location();
        Policy policy = new Policy();
        version.addManifestedAs(encoding);
        version.setCanonicalUri(EMI_PUB_RIGHTS_URI + currentId);
        version.setRestriction(restriction);
        restriction.setMessage(buildRights(currentRights));
        encoding.addAvailableAt(location);
        location.setUri(EMI_PUB_RIGHTS_URI + currentId);
        location.setPolicy(policy);
        policy.setAvailableCountries(Sets.newHashSet(Countries.GB));
        currentSong.setVersions(Sets.newHashSet(version));
        contentWriter.createOrUpdate(currentSong);
    }

    private BufferedReader getReader(File csvFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(csvFile));
        reader.readLine();
        return reader;
    }

    private String buildRights(Map<String, Float> rights) {
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, Float> current : rights.entrySet()) {
            result.append(current.getKey()).append(":").append(current.getValue()).append(",");
        }
        return result.substring(0, result.length() - 1);
    }
}
