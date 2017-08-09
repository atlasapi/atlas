package org.atlasapi.reporting.telescope;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OwlTelescopeProxyTest {

    @Test
    public void mockTelescopeMatchesNormalTelescope() {
        //this is a simple test to ensure OwlTelescopeProxyMock implements all the methods of OwlTelescopeProxy
        Method[] normalMethods = OwlTelescopeProxy.class.getDeclaredMethods();

        //see if all the methods of the normal class, are implemented in the mock class
        for (Method normalMethod : normalMethods) {
            //skip the constructor method
            if(normalMethod.getName().equals("create")){
                continue;
            }
            assertTrue("OwlTelescopeProxyMock does not implement all methods from OwlTelescopeProxy. Missing at least a version of: "+normalMethod.getName(),
                    methodExistsInClass(normalMethod, OwlTelescopeProxyMock.class));
        }
    }

    public static boolean methodExistsInClass(Method method, Class c) {
        Method[] methods = c.getDeclaredMethods();
        boolean same = false;
        for  (int i = 0; i < methods.length && !same; i++){
            if (isSameSignature(method, methods[i])) {
                same = true;
            }
        }
        return same;
    }

    public static boolean isSameSignature(Method methodA, Method methodB) {
        if (methodA == null || methodB == null) {
            return false;
        }

        List parameterTypesA = Arrays.asList(methodA.getParameterTypes());
        List parameterTypesB = Arrays.asList(methodB.getParameterTypes());

        if (methodA.getName().equals(methodB.getName())
            && parameterTypesA.containsAll(parameterTypesB)
            && parameterTypesB.containsAll(parameterTypesA)) {
            return true;
        }

        return false;
    }
}