package org.atlasapi.remotesite.rt.extractors;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;

import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Person;
import org.atlasapi.persistence.logging.NullAdapterLog;

import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.Element;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RtPeopleExtractorTest {

    private Film film;
    private List<Person> peopleList;

    public RtPeopleExtractorTest() {
        film = new Film();
        RtPeopleExtractor peopleExtractor = RtPeopleExtractor.create(getFilmElement(), film, new NullAdapterLog());
        peopleExtractor.process();
        peopleList = peopleExtractor.getPeople();
    }

    private Element getFilmElement() {
        Element rootElement = loadXmlFileFromFile();
        return rootElement.getFirstChildElement("film");
    }

    private Element loadXmlFileFromFile() {
        InputStream rawFile = null;
        try {
            rawFile = new FileInputStream(new File(
                    "src/test/java/org/atlasapi/remotesite/rt/extractors/rtexample.xml"
            ));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Document doc = null;
        try {
            Builder parser = new Builder();
            doc = parser.build(rawFile);
        } catch (Exception e) {
            System.out.println(e.getStackTrace());
        }
        if (doc != null) {
            return doc.getRootElement();
        } else {
            return new Element("element failed to load");
        }
    }

    @Test
    public void personDetailsGetExtracted() {
        for (Person person : peopleList) {
            if (person.getGivenName().equals("Tom")) {
                assertEquals(person.getBilling(), "S");
                assertEquals(person.getFamilyName(), "Hardy");
                assertEquals(person.getPseudoForename(), "Tommy");
                assertEquals(person.getPseudoSurname(), "H");
                assertEquals(person.getAdditionalInfo(), "He is blond");
            } else if (person.getGivenName().equals("George")) {
                assertEquals(person.getFamilyName(), "Miller");
                assertEquals(person.getPseudoSurname(), "M");
                assertEquals(person.getPseudoForename(), "Georgy");
                assertEquals(person.getAdditionalInfo(), "He is not blond");
            } else if (person.getGivenName().equals("Jeanne-Marie")) {
                assertEquals(person.getPseudoForename(), "JM");
                assertEquals(person.getPseudoSurname(), "LPDB");
                assertEquals(person.getSource(), "Fairy tale");
                assertEquals(person.getSourceTitle(), "La Belle et la Be{circumflex}te");
                assertEquals(person.getFamilyName(), "Leprince de Beaumont");
            } else {
                fail("unexpected person found");
            }
        }
        assertEquals(peopleList.size(), 3);
    }
}