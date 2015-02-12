package org.atlasapi.remotesite.knowledgemotion;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

import scala.actors.threadpool.Arrays;

public class KnowledgeMotionCsvTranslatorTest {

    private KnowledgeMotionCsvTranslator csvTranslator;

    private File testCsv;

    @Before
    public void setUp() throws URISyntaxException {
        csvTranslator = new KnowledgeMotionCsvTranslator();

        testCsv = new File(Resources.getResource("knowledgemotion.csv").toURI());
    }

    @Test
    public void translate_parsesFile() throws IOException {
        List<String> keywords = Arrays.asList(new String[] {
            "General News",
            "Banking",
            "Financial Services",
            "U.S. Government",
            "United States",
            "Interviews",
            "Personal Finance-Retirement",
            "Syndication Top US Feed",
            "In the Loop with Betty Liu",
            "Bloomberg.com Web Submit",
            "Retirement Planning",
            "Bloomberg Vid",
            "U.S. TV Source" });

        List<KnowledgeMotionDataRow> rows = csvTranslator.translate(testCsv);

        assertEquals(1, rows.size());

        KnowledgeMotionDataRow row = rows.get(0);
        assertEquals("Bloomberg", row.getSource());
        assertEquals("Bloomberg:4300140444_x01", row.getId());
        assertEquals("Brokers Target Fee Bump in Thrift Savings Rollovers", row.getTitle());
        assertEquals("Aug. 12 - Sarat Sethi, principal and managing director at Douglas C. Lane, and Bloomberg's John Hechinger discuss the practice of brokers luring federal employees and soldiers out of a government retirement program in order to collect high fees on bank plans. They speak on 'In The Loop.'", row.getDescription());
        assertEquals("2014-08-13", row.getDate());
        assertEquals("00:03:17", row.getDuration());
        assertEquals(keywords, row.getKeywords());
        assertEquals("dmd140812ihec.mpg", row.getAlternativeId());
    }

}
