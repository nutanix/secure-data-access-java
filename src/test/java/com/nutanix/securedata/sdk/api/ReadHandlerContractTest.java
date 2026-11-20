/*
 * Copyright (c) 2026 Nutanix Inc. All rights reserved.
 *
 * Author: asis.bagga@nutanix.com
 *
 * Unit tests for the ReadHandler callback contract.
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
 * Guards the published two-callback shape of {@link ReadHandler}. SDK consumers
 * pulling the artifact from a registry rely on there being exactly two callbacks
 * ({@code onChunk} and {@code onError}); this test fails the build if a
 * maintainer adds, removes, or renames one so the change (and its documentation)
 * is made deliberately.
 */
class ReadHandlerContractTest {

    @Test
    void exposesExactlyTheTwoDocumentedCallbacks() {
        List<Method> abstractMethods = Arrays.stream(ReadHandler.class.getDeclaredMethods())
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
                "ReadHandler must expose exactly two callbacks; found: " + signatures);
        assertTrue(signatures.contains("onChunk(RecoveredChunk)"),
                "ReadHandler must declare onChunk(RecoveredChunk); found: " + signatures);
        assertTrue(signatures.contains("onError(ReadError)"),
                "ReadHandler must declare onError(ReadError); found: " + signatures);
    }
}
