/*
 * Copyright (C) 2022 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nl.knaw.dans.validatedansbag;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.configuration.ResourceConfigurationSourceProvider;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.validation.Validators;
import nl.knaw.dans.validatedansbag.config.DdValidateDansBagConfiguration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DdValidateDansBagConfigurationTest {

    private final YamlConfigurationFactory<DdValidateDansBagConfiguration> factory;

    {
        ObjectMapper mapper = Jackson.newObjectMapper().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        factory = new YamlConfigurationFactory<>(DdValidateDansBagConfiguration.class, Validators.newValidator(), mapper, "dw");
    }

    @Test
    public void canReadTest() throws IOException, ConfigurationException {
        factory.build(new ResourceConfigurationSourceProvider(), "debug-etc/config.yml");
    }

    @Test
    public void missingNullAndEmptyBackendsAreAccepted() throws Exception {
        for (var backends : new String[] { "", "dataverse:\nvaultCatalog:\n", "dataverse: {}\nvaultCatalog: {}\n" }) {
            var configuration = readConfig(backends);
            assertThat(configuration.getDataverse()).isNull();
            assertThat(configuration.getVaultCatalog()).isNull();
        }
    }

    @Test
    public void configuredBackendAndEmptyOtherBackendAreAccepted() throws Exception {
        var dataverse = readConfig("dataverse:\n  baseUrl: http://localhost:8080\n  apiKey: test\nvaultCatalog: {}\n");
        assertThat(dataverse.getDataverse().getBaseUrl().toString()).isEqualTo("http://localhost:8080");
        assertThat(dataverse.getVaultCatalog()).isNull();

        var vault = readConfig("dataverse: {}\nvaultCatalog:\n  baseUrl: http://localhost:20300\n");
        assertThat(vault.getDataverse()).isNull();
        assertThat(vault.getVaultCatalog().getBaseUrl().toString()).isEqualTo("http://localhost:20300");
    }

    @Test
    public void bothConfiguredBackendsAreRejected() {
        assertThatThrownBy(() -> readConfig("dataverse:\n  baseUrl: http://localhost:8080\n  apiKey: test\nvaultCatalog:\n  baseUrl: http://localhost:20300\n"))
            .isInstanceOf(ConfigurationException.class);
    }

    @Test
    public void incompleteAndUnknownBackendSettingsAreRejected() {
        assertThatThrownBy(() -> readConfig("vaultCatalog:\n  httpClient: {}\n"))
            .isInstanceOf(ConfigurationException.class);
        assertThatThrownBy(() -> readConfig("dataverse:\n  unknownSetting: true\n"))
            .isInstanceOf(ConfigurationException.class);
    }

    private DdValidateDansBagConfiguration readConfig(String backends) throws Exception {
        String validation;
        try (var stream = getClass().getClassLoader().getResourceAsStream("debug-etc/config.yml")) {
            var yaml = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            validation = yaml.substring(yaml.indexOf("validation:"));
        }
        var yaml = (backends + validation).getBytes(StandardCharsets.UTF_8);
        return factory.build(path -> new ByteArrayInputStream(yaml), "config.yml");
    }
}
