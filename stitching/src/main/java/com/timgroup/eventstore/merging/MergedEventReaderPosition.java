package com.timgroup.eventstore.merging;

import java.io.IOException;
import java.io.StringWriter;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.timgroup.eventstore.api.Position;
import com.timgroup.eventstore.api.PositionCodec;

final class MergedEventReaderPosition implements Position {
    private final String[] names;
    final Position[] inputPositions;

    MergedEventReaderPosition(String[] names, Position[] inputPositions) {
        this.names = names;
        this.inputPositions = inputPositions;
    }

    MergedEventReaderPosition withNextPosition(int readerIndex, Position position) {
        Position[] newPositions = inputPositions.clone();
        newPositions[readerIndex] = position;
        return new MergedEventReaderPosition(names, newPositions);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int index = 0; index < names.length; index++) {
            builder.append(names[index]).append(':').append(inputPositions[index]).append(";");
        }
        builder.setLength(builder.length() - 1);
        return builder.toString();
    }

    static final class MergedEventReaderPositionCodec implements PositionCodec {
        private final JsonFactory jsonFactory = new JsonFactory();
        private final NamedReaderWithCodec[] namedReaders;

        MergedEventReaderPositionCodec(NamedReaderWithCodec[] namedReaders) {
            this.namedReaders = namedReaders;
        }

        @Override
        public String serializePosition(Position position) {
            MergedEventReaderPosition mergedPosition = (MergedEventReaderPosition) position;

            StringWriter stringWriter = new StringWriter();
            try (JsonGenerator jgen = jsonFactory.createGenerator(stringWriter)) {
                jgen.writeStartObject();
                for (int index = 0; index < namedReaders.length; index++) {
                    jgen.writeStringField(namedReaders[index].name, namedReaders[index].codec.serializePosition(mergedPosition.inputPositions[index]));
                }
                jgen.writeEndObject();
            } catch (IOException e) {
                throw new RuntimeException("unable to serialise position", e);
            }
            return stringWriter.toString();
        }

        @Override
        public Position deserializePosition(String serialisedPosition) {
            try (JsonParser jpars = jsonFactory.createParser(serialisedPosition)) {
                if (jpars.nextToken() != JsonToken.START_OBJECT) {
                    throw new IllegalArgumentException("Bad position, expected JSON object, found " + jpars.getCurrentToken() + ": " + serialisedPosition);
                }

                String[] names = new String[namedReaders.length];
                Position[] deserialisedPositions = new Position[namedReaders.length];
                while (jpars.nextToken() == JsonToken.FIELD_NAME) {
                    String name = jpars.getCurrentName();
                    int readerIndex = -1;
                    for (int index = 0; index < namedReaders.length; index++) {
                        //noinspection StringEquality  -- candidate name is interned, Jackson interns field names
                        if (namedReaders[index].name == name) {
                            readerIndex = index;
                            break;
                        }
                    }
                    if (readerIndex < 0) {
                        throw new IllegalArgumentException("Bad position, containing unexpected keys " + serialisedPosition);
                    }
                    if (deserialisedPositions[readerIndex] != null) {
                        throw new IllegalArgumentException("Bad position, containing duplicated keys " + serialisedPosition);
                    }
                    if (jpars.nextToken() != JsonToken.VALUE_STRING) {
                        throw new IllegalArgumentException("Expected JSON string for field \"" + name + "\", found " + jpars.getCurrentToken() + ": " + serialisedPosition);
                    }
                    String valueString = jpars.getText();
                    deserialisedPositions[readerIndex] = namedReaders[readerIndex].codec.deserializePosition(valueString);
                    names[readerIndex] = name;
                }

                if (jpars.getCurrentToken() != JsonToken.END_OBJECT) {
                    throw new IllegalArgumentException("Bad position, expected end of JSON object, found " + jpars.getCurrentToken() + ": " + serialisedPosition);
                }

                for (int i = 0; i < namedReaders.length; i++) {
                    if (deserialisedPositions[i] == null) {
                        throw new IllegalArgumentException("Bad position, containing no key for " + namedReaders[i].name + " :" + serialisedPosition);
                    }
                }

                return new MergedEventReaderPosition(names, deserialisedPositions);
            } catch (IOException e) {
                throw new IllegalArgumentException("unable to deserialise position", e);
            }
        }
    }

}
