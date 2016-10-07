package com.timgroup.eventstore.api;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

public class HasPropertyValueMatcher<O, T> extends TypeSafeDiagnosingMatcher<O> {
    private final PropertyToMatch<O, T> propertyToMatch;
    private final String propertyName;
    private final Matcher<T> expectedValue;

    private HasPropertyValueMatcher(PropertyToMatch<O, T> propertyToMatch, String propertyName, Matcher<T> expectedValue) {
        this.propertyToMatch = propertyToMatch;
        this.propertyName = propertyName;
        this.expectedValue = expectedValue;
    }

    public static <O, T> Matcher<O> hasPropertyValue(PropertyToMatch<O, T> propertyToMatch, Matcher<T> expectedValue) {
        return new HasPropertyValueMatcher<>(propertyToMatch, nameOf(propertyToMatch), expectedValue);
    }

    private static String nameOf(PropertyToMatch<?, ?> property) {
        Optional<String> propertyName = Stream.<Class<?>>iterate(property.getClass(), Class::getSuperclass)
                .flatMap(c -> Arrays.stream(c.getDeclaredMethods()))
                .filter(m -> m.getName().equals("writeReplace"))
                .map(m -> (SerializedLambda) invoke(m, property))
                .map(SerializedLambda::getImplMethodName)
                .findFirst();

        return propertyName.orElseThrow(() -> new RuntimeException("Unable to determine property name."));
    }

    private static Object invoke(Method method, Object param) {
        method.setAccessible(true);
        try {
            return method.invoke(param);
        } catch (IllegalAccessException|InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected boolean matchesSafely(O item, Description mismatchDescription) {
        T actual = propertyToMatch.getFrom(item);
        boolean matches = expectedValue.matches(actual);
        if (!matches) {
            mismatchDescription.appendText(propertyName + " ");
            expectedValue.describeMismatch(actual, mismatchDescription);
            mismatchDescription.appendText(" instead of ");
            expectedValue.describeTo(mismatchDescription);
        }
        return matches;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(propertyName + " ");
        expectedValue.describeTo(description);
    }

    public interface PropertyToMatch<O, T> extends Serializable {
        T getFrom(O eventData);
    }
}