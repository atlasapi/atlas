package org.atlasapi.remotesite.knowledgemotion;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

/**
 * Translates Knowledgemotion's CSVs into KnowledgeMotionDataRows.
 */
public class KnowledgeMotionCsvTranslator {

    private static final Pattern KEYWORDS = Pattern.compile("([^,]+)(, )");

    public List<KnowledgeMotionDataRow> translate(File file) throws IOException {
        Reader reader = new FileReader(file);
        Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(reader);
        List<KnowledgeMotionDataRow> result = new ArrayList<>();
        for (CSVRecord record : records) {
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

            result.add(new KnowledgeMotionDataRow(source, id, title, description, date, duration, keywords, altId));
        }

        return result;
    }

}
