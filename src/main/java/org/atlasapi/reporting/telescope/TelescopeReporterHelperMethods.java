package org.atlasapi.reporting.telescope;

import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.hp.hpl.jena.sparql.pfunction.library.str;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;
import nu.xom.Document;
import nu.xom.Element;
import org.apache.commons.lang3.StringEscapeUtils;
import telescope_client_shaded.com.fasterxml.jackson.databind.util.StdDateFormat;

public class TelescopeReporterHelperMethods {

    private static final Pattern PATTERN_NOT_ENCLOSED = Pattern.compile("^(Entry.|Feed.)([^=\\n]*)=(null|[0-9]+|true|false|\\[\\])$", Pattern.MULTILINE);

    private static final Pattern PATTERN_ENCLOSED = Pattern.compile("^(Entry.|Feed.)([^=\\n]*)=(?!(null|[0-9]+|true|false|\\[\\])$)(.*)$", Pattern.MULTILINE);

    private transient static final ObjectMapper objectMapper = new ObjectMapper()
            .enable(MapperFeature.USE_ANNOTATIONS)
            .configure(MapperFeature.AUTO_DETECT_GETTERS, false)
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
            .setVisibility(PropertyAccessor.CREATOR, JsonAutoDetect.Visibility.NONE)
            .setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE)
            .setVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.NONE)
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            //add the same mixin to every class that suffers from circular references
            .addMixIn(Iterable.class, TelescopeReporterHelperMethods.PreventCircularReferences.class)
            .addMixIn(Element.class, TelescopeReporterHelperMethods.PreventCircularReferences.class)
            .addMixIn(Document.class, TelescopeReporterHelperMethods.PreventCircularReferences.class)
            .registerModule(new JodaModule())
            .setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))
            ;

    /**
     * Serializes all the objects in a single json. The serializer used converts all object fields
     * into json elements. The method offers protection from objects that can't be serialized by not
     * serializing only the failing object. Circular references might cause errors.
     *
     * @param objectsToSerialise
     * @return An empty json, if no objects have been passed in.
     */
    public static String serialize(Object... objectsToSerialise) {

        StringBuilder sb = new StringBuilder().append("{");
        int i = 1; //to differentiate different objects of the same class.
        for (Object o : objectsToSerialise) { //one by one, so what can be serialized will be serialized.
            if (o == null) {
                continue;
            }
            if (i != 1) {
                sb.append(',');
            }
            sb.append("\"Payload-").append(i).append("-").append(o.getClass().getSimpleName()).append("\":");
            try {
                if (o.getClass() != Entry.class && o.getClass() != Feed.class) {
                    sb.append(objectMapper.writeValueAsString(o));
                } else {
                    sb.append("{");
                    sb.append(beautifyEntryAsJsonString(o));
                    sb.append("}");
                }
            } catch (JsonProcessingException e) {
                sb.append("{\"objectMapper\": \"Couldn't convert the given object to a JSON string. (" )
                        .append(StringEscapeUtils.escapeJava(e.getMessage())).append( ")\"}" );
            }
            i++;
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Converts a Feed or Entry to a json string. Values are enclosed in speech marks unless
     * identified as null, numerical, boolean or empty array. Only beatifies at the top level.
     */
    private static String beautifyEntryAsJsonString(Object entry) {
        String str = entry.toString();
        str = PATTERN_NOT_ENCLOSED.matcher(str).replaceAll("\"$2\": $3,");
        str = PATTERN_ENCLOSED.matcher(str).replaceAll("\"$2\": \"$4\",");
        str = str.replaceFirst(",$", ""); // remove last comma
        return str;
    }

    //this is used as mixin to object mapper. It appends the following field to be used as an identifying id for objects.
    @JsonIdentityInfo(generator=ObjectIdGenerators.IntSequenceGenerator.class, property="@PreventCircularReferencesId")
    interface PreventCircularReferences { }
}
