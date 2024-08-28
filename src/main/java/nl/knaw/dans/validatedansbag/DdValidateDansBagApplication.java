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

import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.forms.MultiPartBundle;
import nl.knaw.dans.lib.util.ClientProxyBuilder;
import nl.knaw.dans.lib.util.DataverseHealthCheck;
import nl.knaw.dans.validatedansbag.client.VaultCatalogClientImpl;
import nl.knaw.dans.validatedansbag.config.DdValidateDansBagConfiguration;
import nl.knaw.dans.validatedansbag.config.ValidTermsConfig;
import nl.knaw.dans.validatedansbag.config.ValidTermsFileConfig;
import nl.knaw.dans.validatedansbag.core.engine.RuleEngineImpl;
import nl.knaw.dans.validatedansbag.core.rules.RuleSets;
import nl.knaw.dans.validatedansbag.core.service.BagItMetadataReaderImpl;
import nl.knaw.dans.validatedansbag.core.service.DataverseService;
import nl.knaw.dans.validatedansbag.core.service.DataverseServiceImpl;
import nl.knaw.dans.validatedansbag.core.service.FileServiceImpl;
import nl.knaw.dans.validatedansbag.core.service.FilesXmlServiceImpl;
import nl.knaw.dans.validatedansbag.core.service.OriginalFilepathsServiceImpl;
import nl.knaw.dans.validatedansbag.core.service.RuleEngineServiceImpl;
import nl.knaw.dans.validatedansbag.core.service.VaultCatalogClient;
import nl.knaw.dans.validatedansbag.core.service.XmlReaderImpl;
import nl.knaw.dans.validatedansbag.core.service.XmlSchemaValidatorImpl;
import nl.knaw.dans.validatedansbag.core.validator.IdentifierValidatorImpl;
import nl.knaw.dans.validatedansbag.core.validator.LicenseValidatorImpl;
import nl.knaw.dans.validatedansbag.core.validator.OrganizationIdentifierPrefixValidatorImpl;
import nl.knaw.dans.validatedansbag.core.validator.PolygonListValidatorImpl;
import nl.knaw.dans.validatedansbag.health.XmlSchemaHealthCheck;
import nl.knaw.dans.validatedansbag.resources.IllegalArgumentExceptionMapper;
import nl.knaw.dans.validatedansbag.resources.ValidateOkYamlMessageBodyWriter;
import nl.knaw.dans.validatedansbag.resources.ValidateResource;
import nl.knaw.dans.vaultcatalog.client.invoker.ApiClient;
import nl.knaw.dans.vaultcatalog.client.resources.DefaultApi;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DdValidateDansBagApplication extends Application<DdValidateDansBagConfiguration> {
    public static void main(final String[] args) throws Exception {
        new DdValidateDansBagApplication().run(args);
    }

    @Override
    public String getName() {
        return "Dd Validate Dans Bag";
    }

    @Override
    public void initialize(final Bootstrap<DdValidateDansBagConfiguration> bootstrap) {
        bootstrap.addBundle(new MultiPartBundle());
    }

    @Override
    public void run(final DdValidateDansBagConfiguration configuration, final Environment environment) {
        DataverseService dataverseService = null;

        if (configuration.getDataverse() != null) {
            var dataverseClient = configuration.getDataverse().build(environment, "dd-validate-dans-bag/dataverse");
            dataverseService = new DataverseServiceImpl(dataverseClient);
            environment.healthChecks().register("dataverse", new DataverseHealthCheck(dataverseClient));
        }

        var vaultCatalogClient = getVaultCatalogClient(configuration);

        var fileService = new FileServiceImpl(configuration.getValidation().getBaseFolder());
        var bagItMetadataReader = new BagItMetadataReaderImpl();
        var xmlReader = new XmlReaderImpl();
        var polygonListValidator = new PolygonListValidatorImpl();
        var originalFilepathsService = new OriginalFilepathsServiceImpl(fileService);
        var filesXmlService = new FilesXmlServiceImpl(xmlReader);
        var xmlSchemaValidator = new XmlSchemaValidatorImpl(configuration.getValidation().getXmlSchemas().buildMap());

        var licenseValidator = new LicenseValidatorImpl(dataverseService);
        var identifierValidator = new IdentifierValidatorImpl();
        var organizationIdentifierPrefixValidator = new OrganizationIdentifierPrefixValidatorImpl(configuration.getValidation().getOtherIdPrefixes());

        var schemeUriToValidTerms = loadSchemeUriToValidValues(configuration.getValidation().getValidTerms(), URI::create, ValidTermsFileConfig::getTermsFile);
        var schemeUriToValidCodes = loadSchemeUriToValidValues(configuration.getValidation().getValidTerms(), Function.identity(), ValidTermsFileConfig::getCodesFile);

        var ruleEngine = new RuleEngineImpl();
        var ruleSets = new RuleSets(dataverseService,
            fileService,
            filesXmlService,
            originalFilepathsService,
            xmlReader,
            bagItMetadataReader,
            xmlSchemaValidator,
            licenseValidator,
            identifierValidator,
            polygonListValidator,
            organizationIdentifierPrefixValidator,
            vaultCatalogClient,
            schemeUriToValidTerms,
            schemeUriToValidCodes
        );

        var ruleEngineService = new RuleEngineServiceImpl(ruleEngine, fileService,
            configuration.getDataverse() != null ? ruleSets.getDataStationSet() : ruleSets.getVaasSet());

        environment.jersey().register(new IllegalArgumentExceptionMapper());
        environment.jersey().register(new ValidateResource(ruleEngineService, fileService));
        environment.jersey().register(new ValidateOkYamlMessageBodyWriter());

        environment.healthChecks().register("xml-schemas", new XmlSchemaHealthCheck(xmlSchemaValidator));
    }

    private VaultCatalogClient getVaultCatalogClient(DdValidateDansBagConfiguration configuration) {
        if (configuration.getVaultCatalog() != null) {
            var vaultCatalogProxy = new ClientProxyBuilder<ApiClient, DefaultApi>()
                .apiClient(new ApiClient())
                .basePath(configuration.getVaultCatalog().getBaseUrl())
                .httpClient(configuration.getVaultCatalog().getHttpClient())
                .defaultApiCtor(DefaultApi::new)
                .build();

            return new VaultCatalogClientImpl(vaultCatalogProxy);
        }

        return null;
    }

    private <T> Map<URI, Set<T>> loadSchemeUriToValidValues(ValidTermsConfig validTermsConfig, Function<String, T> converter, Function<ValidTermsFileConfig, Path> filePathExtractor) {
        Map<URI, Set<T>> schemeUriToValidValues = new HashMap<>();

        for (ValidTermsFileConfig validTermsFile : validTermsConfig.getValidTermsFiles()) {
            if (filePathExtractor.apply(validTermsFile) == null) {
                continue;
            }

            schemeUriToValidValues.put(
                validTermsFile.getSchemeUri(),
                loadLinesFromFile(validTermsConfig.getConfigDir().resolve(filePathExtractor.apply(validTermsFile))).stream()
                    .map(converter)
                    .collect(Collectors.toSet()));
        }
        return schemeUriToValidValues;
    }

    private Set<String> loadLinesFromFile(Path file) {
        try {
            return Files.readAllLines(file)
                .stream()
                .skip(1) // skip header
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
        }
        catch (IOException e) {
            throw new IllegalStateException("Could not read file " + file, e);
        }
    }
}
