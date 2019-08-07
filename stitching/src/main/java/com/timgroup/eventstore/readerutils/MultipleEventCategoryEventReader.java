package com.timgroup.eventstore.readerutils;

import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventSource;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public final class MultipleEventCategoryEventReader implements EventReader {
    public static MultipleEventCategoryEventReader curryCategoriesReader(EventSource source, Collection<String> categories) {
        return new MultipleEventCategoryEventReader(source.readAll(), source.readCategory(), categories);
    }

    private final PositionCodec positionCodec;
    private final Position emptyStorePosition;
    private final EventCategoryReader categoryReader;
    private final List<String> categories;

    public MultipleEventCategoryEventReader(EventReader storeReader, EventCategoryReader categoryReader, Collection<String> categories) {
        this.positionCodec = storeReader.storePositionCodec();
        this.emptyStorePosition = storeReader.emptyStorePosition();
        this.categoryReader = requireNonNull(categoryReader);
        this.categories = new ArrayList<>(categories);
    }

    @CheckReturnValue
    @Nonnull
    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return categoryReader.readCategoriesForwards(categories, positionExclusive);
    }

    @Nonnull
    @Override
    public Position emptyStorePosition() {
        return emptyStorePosition;
    }

    @Nonnull
    @Override
    public PositionCodec storePositionCodec() {
        return positionCodec;
    }

    @Override
    public String toString() {
        return "MultipleEventCategoryEventReader{" +
                "categoryReader=" + categoryReader +
                ", categories=" + categories +
                '}';
    }
}
