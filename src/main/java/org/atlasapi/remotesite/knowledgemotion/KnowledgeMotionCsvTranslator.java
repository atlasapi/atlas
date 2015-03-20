package org.atlasapi.remotesite.knowledgemotion;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.google.common.collect.ImmutableList;

/**
 * Translates Knowledgemotion's CSVs into KnowledgeMotionDataRows.
 */
public class KnowledgeMotionCsvTranslator {

    private static final String[] HEADER = new String[] {
        "Source",
        "Unique ID",
        "namespace",
        "Title",
        "Description",
        "Date",
        "Duration",
        "Keywords",
        "Price category (1= stock, 2=news, 3=brand)",
        "Sounds",
        "Color",
        "Location",
        "Country",
        "State",
        "City",
        "Region",
        "Alternative ID" };
    private static final Pattern KEYWORDS = Pattern.compile("([^,]+)(, )?");

    /**
     * Knowledgemotion's CSVs contain two 'Source' columns (the first column and last column).
     * CSVFormat will not parse the header because of this. Therefore, we skip the header row with
     * records.next() and use our own header row defined as a class constant, which will cause
     * the last column to be ignored. This method will continue to work in the same way if the
     * second column is removed.
     */
    public List<KnowledgeMotionDataRow> translate(File file) throws IOException {
        Reader reader = new FileReader(file);
        Iterator<CSVRecord> records = CSVFormat.RFC4180.withHeader(HEADER).parse(reader).iterator();
        records.next();
        ImmutableList.Builder<KnowledgeMotionDataRow> resultBuilder = ImmutableList.builder();
        while (records.hasNext()) {
            CSVRecord record = records.next();

            String source = record.get(KnowledgeMotionSpreadsheetColumn.SOURCE.getFieldName());
            String id = record.get(KnowledgeMotionSpreadsheetColumn.ID.getFieldName());
            String title = record.get(KnowledgeMotionSpreadsheetColumn.TITLE.getFieldName());
            String description = record.get(KnowledgeMotionSpreadsheetColumn.DESCRIPTION.getFieldName());
            String date = record.get(KnowledgeMotionSpreadsheetColumn.DATE.getFieldName());
            String duration = record.get(KnowledgeMotionSpreadsheetColumn.DURATION.getFieldName());
            String keywordsString = record.get(KnowledgeMotionSpreadsheetColumn.KEYWORDS.getFieldName());
            String altId = record.get(KnowledgeMotionSpreadsheetColumn.ALT_ID.getFieldName());

            List<String> keywords = new ArrayList<>();
            Matcher keywordMatcher = KEYWORDS.matcher(keywordsString);
            while (keywordMatcher.find()) {
                keywords.add(keywordMatcher.group(1));
            }

            resultBuilder.add(new KnowledgeMotionDataRow(source, id, title, description, date, duration, keywords, altId));
        }

        return resultBuilder.build();
    }

}
