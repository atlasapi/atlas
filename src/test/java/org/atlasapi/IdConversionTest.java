package org.atlasapi;

import java.math.BigInteger;
import java.util.Set;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;


public class IdConversionTest {

    private final NumberToShortStringCodec lowerCase = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final NumberToShortStringCodec upperCase = new SubstitutionTableNumberCodec();
    
    @Test
    public void testLowerCaseDecoding() {
        Set<String> stringids = ImmutableSet.of("hkzf","hkzv","hky6","hky4","hk2m");
        
        for (String strId : stringids) {
            System.out.print("NumberLong(" + lowerCase.decode(strId) + "), ");
        }
    }

    @Test
    public void testUpperCaseDecoding() {
        Set<String> stringids = ImmutableSet.of("ckrnh");
        
        for (String strId : stringids) {
            System.out.print("NumberLong(" + upperCase.decode(strId) + "), ");
        }
    }
    
    @Test
    public void testToString() {
        Set<Long> ids = ImmutableSet.of(104261l);
        
        for (Long id : ids) {
            System.out.println(id + " " + lowerCase.encode(BigInteger.valueOf(id)) + " " + upperCase.encode(BigInteger.valueOf(id)));
        }
    }
    
}
