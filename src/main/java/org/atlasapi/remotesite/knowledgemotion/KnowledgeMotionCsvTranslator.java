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

import com.google.common.base.Splitter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.google.common.collect.ImmutableList;

import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.ALT_ID;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.DATE;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.DESCRIPTION;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.DURATION;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.ID;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.PRICE_CATEGORY;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.SOURCE;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.TERMS_OF_USE;
import static org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionSpreadsheetColumn.TITLE;

/**
 * Translates Knowledgemotion's CSVs into KnowledgeMotionDataRows.
 */
public class KnowledgeMotionCsvTranslator {

    private static final String[] HEADER = new String[] {
        SOURCE.getFieldName(),
        "Unique ID",
        ID.getFieldName(),
        TITLE.getFieldName(),
        DESCRIPTION.getFieldName(),
        DATE.getFieldName(),
        DURATION.getFieldName(),
        KnowledgeMotionSpreadsheetColumn.KEYWORDS.getFieldName(),
        PRICE_CATEGORY.getFieldName(),
        "Sounds",
        "Color",
        "Location",
        "Country",
        "State",
        "City",
        "Region",
        ALT_ID.getFieldName(),
        TERMS_OF_USE.getFieldName()
    };
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

            String source = record.get(SOURCE.getFieldName());
            String id = record.get(ID.getFieldName());
            String title = record.get(TITLE.getFieldName());
            String description = record.get(DESCRIPTION.getFieldName());
            String date = record.get(DATE.getFieldName());
            String duration = record.get(DURATION.getFieldName());
            String keywordsString = record.get(KnowledgeMotionSpreadsheetColumn.KEYWORDS.getFieldName());
            Iterable<String> priceCategories = Splitter.on(",")
                    .omitEmptyStrings()
                    .split(record.get(PRICE_CATEGORY.getFieldName()));
            String altId = record.get(ALT_ID.getFieldName());
            String termsOfUse = record.get(TERMS_OF_USE.getFieldName());

            List<String> keywords = new ArrayList<>();
            Matcher keywordMatcher = KEYWORDS.matcher(keywordsString);
            while (keywordMatcher.find()) {
                keywords.add(keywordMatcher.group(1));
            }

            resultBuilder.add(
                    new KnowledgeMotionDataRow(
                            source,
                            id,
                            title,
                            description,
                            date,
                            duration,
                            keywords,
                            priceCategories,
                            altId,
                            termsOfUse
                    )
            );
        }

        return resultBuilder.build();
    }

}
