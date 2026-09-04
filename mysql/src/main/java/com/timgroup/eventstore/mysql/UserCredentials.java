package com.timgroup.eventstore.mysql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;

import java.io.IOException;
import java.util.Objects;

import static java.lang.String.format;

final class UserCredentials {
    private static final SecretsManagerClient secretsManagerClient = SecretsManagerClient.builder().build();
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new ParameterNamesModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    public static UserCredentials fetch(String secretId) {
        GetSecretValueResponse response;
        try {
            response = secretsManagerClient.getSecretValue(r -> r.secretId(secretId));
        } catch (ResourceNotFoundException e) {
            throw new IllegalArgumentException(format("Secret ID '%s' does not exist", secretId), e);
        }
        try {
            return objectMapper.readValue(response.secretString(), UserCredentials.class);
        } catch (IOException e) {
            throw new RuntimeException(format("Invalid user credentials in secret '%s'", secretId), e);
        }
    }

    public final String username;
    public final String password;

    @JsonCreator
    private UserCredentials(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        UserCredentials that = (UserCredentials) o;
        return Objects.equals(username, that.username) && Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, password);
    }
}
