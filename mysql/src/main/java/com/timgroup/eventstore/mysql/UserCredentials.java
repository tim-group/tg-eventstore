package com.timgroup.eventstore.mysql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;

import java.io.IOException;
import java.util.Objects;

import static java.lang.String.format;

@JsonIgnoreProperties(ignoreUnknown = true)
final class UserCredentials {
    private static final SecretsManagerClient secretsManagerClient = SecretsManagerClient.builder().build();
    private static final ObjectMapper objectMapper = new ObjectMapper();

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
    private UserCredentials(@JsonProperty("username") String username, @JsonProperty("password") String password) {
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
