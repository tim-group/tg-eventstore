package com.timgroup.eventstore.mysql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import java.io.IOException;
import java.util.Optional;
import java.util.regex.Pattern;

final class CredentialsInSecret {
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new ParameterNamesModule())
            .registerModule(new Jdk8Module());

    private static final Pattern PREFIX_PATTERN = Pattern.compile("^\\s*\\{\\s*\"}");
    private static final Pattern SUFFIX_PATTERN = Pattern.compile("\\s*}\\s*$");

    public static Optional<CredentialsInSecret> extract(String secretString) {
        if (!PREFIX_PATTERN.matcher(secretString).find()) return Optional.empty();
        if (!SUFFIX_PATTERN.matcher(secretString).find()) return Optional.empty();
        if (!secretString.startsWith("{") || !secretString.endsWith("}")) return Optional.empty();
        try {
            return Optional.of(objectMapper.readValue(secretString, CredentialsInSecret.class));
        } catch (JsonParseException e) {
            throw new RuntimeException("Credentials secret string looked a bit like JSON, but it wasn't", e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public final String username;
    public final String password;

    @JsonCreator
    public CredentialsInSecret(String username, String password) {
        this.username = username;
        this.password = password;
    }
}
