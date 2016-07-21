package org.atlasapi.remotesite.bbc.nitro;

import java.io.StringWriter;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.atlasapi.remotesite.bbc.nitro.extract.NitroItemSource;

import com.metabroadcast.atlas.glycerin.model.Availability;
import com.metabroadcast.atlas.glycerin.model.Broadcast;
import com.metabroadcast.atlas.glycerin.model.Clip;
import com.metabroadcast.atlas.glycerin.model.Episode;
import com.metabroadcast.atlas.glycerin.model.Version;

import com.google.common.base.Throwables;

public class GlycerinNitroPayloadXmlSerializer {

    private final JAXBContext jaxbContext;
    private final Marshaller marshaller;

    public static GlycerinNitroPayloadXmlSerializer create() {
        return new GlycerinNitroPayloadXmlSerializer();
    }

    private GlycerinNitroPayloadXmlSerializer() {
        try {
            jaxbContext = JAXBContext.newInstance(
                    com.metabroadcast.atlas.glycerin.model.Clip.class,
                    Version.class,
                    Availability.class,
                    Episode.class
            );
            marshaller = jaxbContext.createMarshaller();
        } catch (JAXBException e) {
            throw Throwables.propagate(e);
        }
    }

    public String marshal(NitroItemSource<Episode> episode, List<NitroItemSource<Clip>> clips) {
        try {
            StringWriter sw = new StringWriter();

            sw.write("<entity>");
            writeNitroSourceXml(episode, marshaller, sw);

            sw.write("<clips>");
            for (NitroItemSource<com.metabroadcast.atlas.glycerin.model.Clip> clip : clips) {
                sw.write("<clip>");
                writeNitroSourceXml(clip, marshaller, sw);
                sw.write("</clip>");
            }
            sw.write("</clips>");

            sw.write("</entity>");

            return sw.toString();
        } catch (JAXBException e) {
            throw Throwables.propagate(e);
        }
    }

    private <T> void writeNitroSourceXml(
            NitroItemSource<T> source,
            Marshaller marshaller,
            StringWriter sw
    ) throws JAXBException {
        marshaller.marshal(source.getProgramme(), sw);

        sw.write("<versions>");
        for (Version version : source.getVersions()) {
            marshaller.marshal(version, sw);
        }
        sw.write("</versions>");

        sw.write("<availabilities>");
        for (Availability availability : source.getAvailabilities()) {
            marshaller.marshal(availability, sw);
        }
        sw.write("</availabilities>");

        sw.write("<broadcasts>");
        for (Broadcast broadcast : source.getBroadcasts()) {
            marshaller.marshal(broadcast, sw);
        }
        sw.write("</broadcasts>");
    }
}
