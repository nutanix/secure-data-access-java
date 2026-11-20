/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Unit tests for the WriteHandler callback contract.
 */
package com.nutanix.securedata.sdk.api;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the published two-callback shape of {@link WriteHandler}. SDK consumers
 * pulling the artifact from a registry rely on there being exactly two callbacks
 * ({@code onSuccess} and {@code onError}); this test fails the build if a
 * maintainer adds, removes, or renames one so the change (and its documentation)
 * is made deliberately.
 */
class WriteHandlerContractTest {

    @Test
    void exposesExactlyTheTwoDocumentedCallbacks() {
        List<Method> abstractMethods = Arrays.stream(WriteHandler.class.getDeclaredMethods())
                .filter(m -> Modifier.isAbstract(m.getModifiers()))
                .collect(Collectors.toList());

        Set<String> signatures = abstractMethods.stream()
                .map(m -> m.getName() + "("
                        + Arrays.stream(m.getParameterTypes())
                                .map(Class::getSimpleName)
                                .collect(Collectors.joining(","))
                        + ")")
                .collect(Collectors.toSet());

        assertEquals(2, abstractMethods.size(),
                "WriteHandler must expose exactly two callbacks; found: " + signatures);
        assertTrue(signatures.contains("onSuccess(WriteAck)"),
                "WriteHandler must declare onSuccess(WriteAck); found: " + signatures);
        assertTrue(signatures.contains("onError(WriteError)"),
                "WriteHandler must declare onError(WriteError); found: " + signatures);
    }
}
