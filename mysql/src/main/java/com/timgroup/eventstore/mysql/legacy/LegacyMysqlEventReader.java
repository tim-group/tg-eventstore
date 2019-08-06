package com.timgroup.eventstore.mysql.legacy;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.timgroup.eventstore.api.EventCategoryReader;
import com.timgroup.eventstore.api.EventReader;
import com.timgroup.eventstore.api.EventStreamReader;
import com.timgroup.eventstore.api.NoSuchStreamException;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;
import com.timgroup.eventstore.api.ResolvedEvent;
import com.timgroup.eventstore.api.StreamId;
import com.timgroup.eventstore.mysql.ConnectionProvider;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.StreamSupport.stream;

public final class LegacyMysqlEventReader implements EventReader, EventStreamReader, EventCategoryReader {

    private final ConnectionProvider connectionProvider;
    private final String tableName;
    private final StreamId pretendStreamId;
    private final int batchSize;
    private final Optional<Timer> timer;

    public LegacyMysqlEventReader(ConnectionProvider connectionProvider, String database, String tableName, StreamId pretendStreamId, int batchSize, @Nullable MetricRegistry metricRegistry) {
        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
        this.pretendStreamId = pretendStreamId;
        this.batchSize = batchSize;
        this.timer = Optional.ofNullable(metricRegistry).map(r -> r.timer(String.format("database.%s.%s.read.page_fetch_time", database, tableName)));
    }

    @Nonnull
    @Override
    public Position emptyStorePosition() {
        return LegacyMysqlEventPosition.fromLegacyVersion(0);
    }

    @Nonnull
    @Override
    public Position emptyCategoryPosition(String category) {
        return emptyStorePosition();
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readAllForwards() {
        return readAllForwards(emptyStorePosition());
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readAllForwards(Position positionExclusive) {
        return stream(
                new LegacyMysqlEventSpliterator(
                    connectionProvider,
                    batchSize,
                    tableName,
                    pretendStreamId,
                    (LegacyMysqlEventPosition)positionExclusive,
                    false,
                        timer),
                false
        );
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readStreamForwards(StreamId streamId, long eventNumber) {
        if (!streamId.equals(pretendStreamId)) {
            throw new IllegalArgumentException("Cannot read " + streamId + " from legacy store");
        }
        ensureStreamExists(streamId);
        return readAllForwards(LegacyMysqlEventPosition.fromEventNumber(eventNumber));
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readCategoryForwards(String category, Position positionExclusive) {
        if (!category.equals(pretendStreamId.category())) {
            throw new IllegalArgumentException("Cannot read " + category + " from legacy store");
        }
        return readAllForwards(positionExclusive);
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readAllBackwards() {
        return readAllBackwards(LegacyMysqlEventPosition.fromLegacyVersion(Long.MAX_VALUE));
    }

    @Nonnull
    @Override
    public Optional<ResolvedEvent> readLastEvent() {
        return readBackwards(LegacyMysqlEventPosition.fromLegacyVersion(Long.MAX_VALUE), 1).findFirst();
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readAllBackwards(Position positionExclusive) {
        return readBackwards((LegacyMysqlEventPosition) positionExclusive, this.batchSize);
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readCategoryBackwards(String category) {
        return readCategoryBackwards(category, LegacyMysqlEventPosition.fromLegacyVersion(Long.MAX_VALUE));
    }

    @Nonnull
    @Override
    public Optional<ResolvedEvent> readLastEventInCategory(String category) {
        if (!category.equals(pretendStreamId.category())) {
            throw new IllegalArgumentException("Cannot read " + category + " from legacy store");
        }
        return readLastEvent();
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readCategoryBackwards(String category, Position positionExclusive) {
        if (!category.equals(pretendStreamId.category())) {
            throw new IllegalArgumentException("Cannot read " + category + " from legacy store");
        }
        return readAllBackwards(positionExclusive);
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readStreamBackwards(StreamId streamId) {
        return readStreamBackwards(streamId, LegacyMysqlEventPosition.fromLegacyVersion(Long.MAX_VALUE));
    }

    @Nonnull
    @CheckReturnValue
    @Override
    public Stream<ResolvedEvent> readStreamBackwards(StreamId streamId, long eventNumber) {
        return readStreamBackwards(streamId, LegacyMysqlEventPosition.fromEventNumber(eventNumber));
    }

    @Nonnull
    @Override
    public ResolvedEvent readLastEventInStream(StreamId streamId) {
        if (!streamId.equals(pretendStreamId)) {
            throw new IllegalArgumentException("Cannot read " + streamId + " from legacy store");
        }
        ensureStreamExists(streamId);
        //noinspection ConstantConditions
        return readLastEvent().get();
    }

    @Nonnull
    @Override
    public PositionCodec categoryPositionCodec(String category) {
        return LegacyMysqlEventPosition.CODEC;
    }

    @Nonnull
    @Override
    public PositionCodec storePositionCodec() {
        return LegacyMysqlEventPosition.CODEC;
    }

    @Nonnull
    @Override
    public PositionCodec streamPositionCodec() {
        return LegacyMysqlEventPosition.CODEC;
    }

    @Override
    public String toString() {
        return "LegacyMysqlEventReader{" +
                "tableName='" + tableName + '\'' +
                ", pretendStreamId=" + pretendStreamId +
                ", batchSize=" + batchSize +
                '}';
    }

    private Stream<ResolvedEvent> readBackwards(LegacyMysqlEventPosition positionExclusive, int theBatchSize) {
        return stream(
                new LegacyMysqlEventSpliterator(
                        connectionProvider,
                        theBatchSize,
                        tableName,
                        pretendStreamId,
                        positionExclusive,
                        true,
                        timer

                ),
                false
        );
    }

    private Stream<ResolvedEvent> readStreamBackwards(StreamId streamId, LegacyMysqlEventPosition position) {
        if (!streamId.equals(pretendStreamId)) {
            throw new IllegalArgumentException("Cannot read " + streamId + " from legacy store");
        }
        ensureStreamExists(streamId);
        return readAllBackwards(position);
    }

    private void ensureStreamExists(StreamId streamId) throws NoSuchStreamException {
        try (Connection connection = connectionProvider.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(String.format("select version from %s limit 1", tableName))
        ) {
            if (!resultSet.first()) {
                throw new NoSuchStreamException(streamId);
            }
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Error checking whether stream '%s' exists", streamId), e);
        }
    }
}
