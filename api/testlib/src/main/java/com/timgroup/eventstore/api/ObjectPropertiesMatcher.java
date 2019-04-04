package com.timgroup.eventstore.api;

import com.timgroup.eventstore.api.HasPropertyValueMatcher.PropertyToMatch;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.ArrayList;
import java.util.List;

import static com.timgroup.eventstore.api.HasPropertyValueMatcher.hasPropertyValue;
import static org.hamcrest.Matchers.equalTo;

public class ObjectPropertiesMatcher<T> extends TypeSafeDiagnosingMatcher<T> {
    private final List<Matcher<? super T>> properties = new ArrayList<>();

    public static <T, R> ObjectPropertiesMatcher<T> objectWith(PropertyToMatch<? super T, ? extends R> property, R expectedValue) {
        ObjectPropertiesMatcher<T> matcher = new ObjectPropertiesMatcher<>();
        return matcher.and(property, expectedValue);
    }

    public <R> ObjectPropertiesMatcher<T> and(PropertyToMatch<? super T, ? extends R> property, R expectedValue) {
        return andMatching(property, equalTo(expectedValue));
    }

    public <R> ObjectPropertiesMatcher<T> andMatching(PropertyToMatch<? super T, ? extends R> property, Matcher<? super R> expectedValue) {
        properties.add(hasPropertyValue(property, expectedValue));
        return this;
    }

    @Override
    protected boolean matchesSafely(T item, Description mismatchDescription) {
        properties.forEach(p -> {
            mismatchDescription.appendText("\n\t");
            p.describeMismatch(item, mismatchDescription);
        });
        return properties.stream().allMatch(p -> p.matches(item));
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("{");
        properties.forEach(p -> {
            description.appendText("\n\t");
            p.describeTo(description);
        });
        description.appendText("\n}");
    }
}
