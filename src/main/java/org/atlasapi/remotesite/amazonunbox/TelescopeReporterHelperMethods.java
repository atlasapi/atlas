package org.atlasapi.remotesite.amazonunbox;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.lang3.StringEscapeUtils;

public class TelescopeReporterHelperMethods {
    private transient static final ObjectMapper objectMapper = new ObjectMapper()
            .enable(MapperFeature.USE_ANNOTATIONS)
            .configure(MapperFeature.AUTO_DETECT_GETTERS, false)
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
            .setVisibility(PropertyAccessor.CREATOR, JsonAutoDetect.Visibility.NONE)
            .setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE)
            .setVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.NONE)
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            //add the same mixin to every class that suffers from circular references
            .addMixIn(Iterable.class, org.atlasapi.remotesite.amazonunbox.TelescopeReporterHelperMethods.PreventCircularReferences.class);

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
            if (i != 1) {
                sb.append(',');
            }
            sb.append("\"Payload-").append(i).append("-").append(o.getClass().getSimpleName()).append("\":");
            try {
                sb.append(objectMapper.writeValueAsString(o));
            } catch (JsonProcessingException e) {
                sb.append("{\"objectMapper\": \"Couldn't convert the given object to a JSON string. (" )
                        .append(StringEscapeUtils.escapeJava(e.getMessage())).append( ")\"}" );
            }
            i++;
        }
        sb.append("}");
        return sb.toString();
    }

    //this is used as mixin to object mapper. It appends the following field to be used as an identifying id for objects.
    @JsonIdentityInfo(generator=ObjectIdGenerators.IntSequenceGenerator.class, property="@PreventCircularReferencesId")
    interface PreventCircularReferences { }
}
