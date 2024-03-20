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
package nl.knaw.dans.validatedansbag.resources;

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.model.DataMessage;
import nl.knaw.dans.lib.dataverse.model.RoleAssignmentReadOnly;
import nl.knaw.dans.lib.dataverse.model.dataset.DatasetLatestVersion;
import nl.knaw.dans.lib.dataverse.model.search.SearchResult;
import nl.knaw.dans.validatedansbag.api.ValidateCommandDto;
import nl.knaw.dans.validatedansbag.api.ValidateOkDto;
import nl.knaw.dans.validatedansbag.api.ValidateOkRuleViolationsInnerDto;
import nl.knaw.dans.validatedansbag.core.engine.RuleEngineImpl;
import nl.knaw.dans.validatedansbag.core.rules.RuleSets;
import nl.knaw.dans.validatedansbag.core.service.BagItMetadataReaderImpl;
import nl.knaw.dans.validatedansbag.core.service.DataverseService;
import nl.knaw.dans.validatedansbag.core.service.FileServiceImpl;
import nl.knaw.dans.validatedansbag.core.service.FilesXmlServiceImpl;
import nl.knaw.dans.validatedansbag.core.service.OriginalFilepathsServiceImpl;
import nl.knaw.dans.validatedansbag.core.service.RuleEngineServiceImpl;
import nl.knaw.dans.validatedansbag.core.service.VaultCatalogClient;
import nl.knaw.dans.validatedansbag.core.service.XmlReaderImpl;
import nl.knaw.dans.validatedansbag.core.service.XmlSchemaValidator;
import nl.knaw.dans.validatedansbag.core.validator.IdentifierValidatorImpl;
import nl.knaw.dans.validatedansbag.core.validator.LicenseValidator;
import nl.knaw.dans.validatedansbag.core.validator.OrganizationIdentifierPrefixValidatorImpl;
import nl.knaw.dans.validatedansbag.core.validator.PolygonListValidatorImpl;
import nl.knaw.dans.validatedansbag.resources.util.MockedDataverseResponse;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.xml.sax.SAXException;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static nl.knaw.dans.validatedansbag.resources.util.TestUtil.basicUsernamePassword;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(DropwizardExtensionsSupport.class)
class ValidateResourceIntegrationTest {
    public static final ResourceExtension EXT;

    private static final DataverseService dataverseService = Mockito.mock(DataverseService.class);
    private static final XmlSchemaValidator xmlSchemaValidator = Mockito.mock(XmlSchemaValidator.class);
    private static final String baseTestFolder = Objects.requireNonNull(Objects.requireNonNull(ValidateResourceIntegrationTest.class.getClassLoader().getResource("")).getPath());

    private static final LicenseValidator licenseValidator = new LicenseValidator() {

        @Override
        public boolean isValidUri(String license) {
            return true;
        }

        @Override
        public boolean isValidLicense(String license)  {
            return true;
        }
    };

    static {
        EXT = ResourceExtension.builder()
                .addProvider(MultiPartFeature.class)
                .addProvider(ValidateOkYamlMessageBodyWriter.class)
                .addResource(buildValidateResource())
                .build();
    }

    static ValidateResource buildValidateResource() {
        var fileService = new FileServiceImpl(Path.of(baseTestFolder));
        var bagItMetadataReader = new BagItMetadataReaderImpl();
        var xmlReader = new XmlReaderImpl();
        var polygonListValidator = new PolygonListValidatorImpl();
        var originalFilepathsService = new OriginalFilepathsServiceImpl(fileService);
        var filesXmlService = new FilesXmlServiceImpl(xmlReader);
        var identifierValidator = new IdentifierValidatorImpl();
        var vaultService = Mockito.mock(VaultCatalogClient.class);

        var organizationIdentifierPrefixValidator = new OrganizationIdentifierPrefixValidatorImpl(
                List.of("u1:", "u2:")
        );

        // set up the engine and the service that has a default set of rules
        var ruleEngine = new RuleEngineImpl();
        var ruleSets = new RuleSets(
                dataverseService, fileService, filesXmlService, originalFilepathsService, xmlReader,
                bagItMetadataReader, xmlSchemaValidator, licenseValidator, identifierValidator, polygonListValidator, organizationIdentifierPrefixValidator,
                vaultService);

        var ruleEngineService = new RuleEngineServiceImpl(ruleEngine, fileService, ruleSets.getDataStationSet());
        return new ValidateResource(ruleEngineService, fileService);
    }

    @BeforeEach
    void setup() {
        Mockito.reset(dataverseService);
        Mockito.reset(xmlSchemaValidator);
    }

    @Test
    void validateFormData_should_have_validation_errors_with_invalid_bag() throws IOException, DataverseException {
        var filename = baseTestFolder + "/bags/audiences-invalid";

        var data = new ValidateCommandDto();
        data.setBagLocation(filename);
        data.setPackageType(ValidateCommandDto.PackageTypeEnum.DEPOSIT);
        var multipart = new FormDataMultiPart()
                .field("command", data, MediaType.APPLICATION_JSON_TYPE);

        var embargoResultJson = """
                {
                  "status": "OK",
                  "data": {
                    "message": "24"
                  }
                }""";
        var maxEmbargoDurationResult = new MockedDataverseResponse<DataMessage>(embargoResultJson, DataMessage.class);
        Mockito.when(dataverseService.getMaxEmbargoDurationInMonths())
                .thenReturn(maxEmbargoDurationResult);

        var response = EXT.target("/validate")
                .register(MultiPartFeature.class)
                .request()
                .post(Entity.entity(multipart, multipart.getMediaType()), ValidateOkDto.class);

        assertFalse(response.getIsCompliant());
        assertEquals("1.0.0", response.getProfileVersion());
        assertEquals(ValidateOkDto.InformationPackageTypeEnum.DEPOSIT, response.getInformationPackageType());
        assertEquals(filename, response.getBagLocation());
        assertFalse(response.getRuleViolations().isEmpty());
    }

    @Test
    void validateFormData_should_return_500_when_xml_errors_occur() throws Exception {
        var filename = baseTestFolder + "/bags/valid-bag";

        var data = new ValidateCommandDto();
        data.setBagLocation(filename);
        data.setPackageType(ValidateCommandDto.PackageTypeEnum.DEPOSIT);
        var multipart = new FormDataMultiPart()
                .field("command", data, MediaType.APPLICATION_JSON_TYPE);

        Mockito.when(xmlSchemaValidator.validateDocument(Mockito.any(), Mockito.anyString()))
                .thenThrow(new SAXException("Something is broken"));

        try (var response = EXT.target("/validate")
                .register(MultiPartFeature.class)
                .request()
                .post(Entity.entity(multipart, multipart.getMediaType()), Response.class)) {

            assertEquals(500, response.getStatus());
        }
    }

    @Test
    void validateFormData_should_validate_ok_with_valid_bag_and_original_filepaths() throws Exception {
        var filename = baseTestFolder + "/bags/datastation-valid-bag";

        var data = new ValidateCommandDto();
        data.setBagLocation(filename);
        data.setPackageType(ValidateCommandDto.PackageTypeEnum.MIGRATION);

        var multipart = new FormDataMultiPart()
                .field("command", data, MediaType.APPLICATION_JSON_TYPE);

        var searchResultsJson = """
                {
                  "status": "OK",
                  "data": {
                    "q": "NBN:urn:nbn:nl:ui:13-025de6e2-bdcf-4622-b134-282b4c590f42",
                    "total_count": 1,
                    "start": 0,
                    "spelling_alternatives": {},
                    "items": [
                      {
                        "name": "Manual Test",
                        "type": "dataset",
                        "url": "https://doi.org/10.5072/FK2/QZZSST",
                        "global_id": "doi:10.5072/FK2/QZZSST"
                      }
                    ],
                    "count_in_response": 1
                  }
                }""";

        var dataverseRoleAssignmentsJson = """
                {
                  "status": "OK",
                  "data": [
                    {
                      "id": 6,
                      "assignee": "@user001",
                      "roleId": 11,
                      "_roleAlias": "datasetcreator",
                      "definitionPointId": 2
                    }
                  ]
                }""";

        var embargoResultJson = """
                {
                  "status": "OK",
                  "data": {
                    "message": "24"
                  }
                }""";

        var swordTokenResult = new MockedDataverseResponse<SearchResult>(searchResultsJson, SearchResult.class);
        var dataverseRoleAssignmentsResult = new MockedDataverseResponse<List<RoleAssignmentReadOnly>>(dataverseRoleAssignmentsJson, List.class, RoleAssignmentReadOnly.class);
        var maxEmbargoDurationResult = new MockedDataverseResponse<DataMessage>(embargoResultJson, DataMessage.class);

        Mockito.when(dataverseService.searchBySwordToken(Mockito.anyString()))
                .thenReturn(swordTokenResult);

        Mockito.when(dataverseService.getDataverseRoleAssignments(Mockito.anyString()))
                .thenReturn(dataverseRoleAssignmentsResult);

        Mockito.when(dataverseService.getMaxEmbargoDurationInMonths())
                .thenReturn(maxEmbargoDurationResult);

        var response = EXT.target("/validate")
                .register(MultiPartFeature.class)
                .request()
                .post(Entity.entity(multipart, multipart.getMediaType()), ValidateOkDto.class);

        assertTrue(response.getIsCompliant());
        assertEquals("1.0.0", response.getProfileVersion());
        assertEquals(ValidateOkDto.InformationPackageTypeEnum.MIGRATION, response.getInformationPackageType());
        assertEquals(filename, response.getBagLocation());
        assertEquals(0, response.getRuleViolations().size());
    }

    @Test
    void validateFormData_should_have_validation_errors_with_invalid_bag_and_original_filepaths() throws Exception {
        var filename = baseTestFolder + "/bags/original-filepaths-invalid-bag";

        var data = new ValidateCommandDto();
        data.setBagLocation(filename);
        data.setPackageType(ValidateCommandDto.PackageTypeEnum.MIGRATION);

        var multipart = new FormDataMultiPart()
                .field("command", data, MediaType.APPLICATION_JSON_TYPE);

        var embargoResultJson = """
                {
                  "status": "OK",
                  "data": {
                    "message": "24"
                  }
                }""";

        var maxEmbargoDurationResult = new MockedDataverseResponse<DataMessage>(embargoResultJson, DataMessage.class);

        Mockito.when(dataverseService.getMaxEmbargoDurationInMonths())
                .thenReturn(maxEmbargoDurationResult);

        var response = EXT.target("/validate")
                .register(MultiPartFeature.class)
                .request()
                .post(Entity.entity(multipart, multipart.getMediaType()), ValidateOkDto.class);

        assertFalse(response.getIsCompliant());
        assertEquals("1.0.0", response.getProfileVersion());
        assertEquals(ValidateOkDto.InformationPackageTypeEnum.MIGRATION, response.getInformationPackageType());
        assertEquals(filename, response.getBagLocation());
    }

    @Test
    void validateFormData_should_have_validation_errors_with_not_allowed_original_metadata_zip() throws Exception {
        var filename = baseTestFolder + "/bags/bag-with-original-metadata-zip";

        var data = new ValidateCommandDto();
        data.setBagLocation(filename);
        data.setPackageType(ValidateCommandDto.PackageTypeEnum.DEPOSIT);
        var multipart = new FormDataMultiPart()
                .field("command", data, MediaType.APPLICATION_JSON_TYPE);

        var embargoResultJson = """
                {
                  "status": "OK",
                  "data": {
                    "message": "24"
                  }
                }""";
        var maxEmbargoDurationResult = new MockedDataverseResponse<DataMessage>(embargoResultJson, DataMessage.class);
        Mockito.when(dataverseService.getMaxEmbargoDurationInMonths())
                .thenReturn(maxEmbargoDurationResult);

        var response = EXT.target("/validate")
                .register(MultiPartFeature.class)
                .request()
                .post(Entity.entity(multipart, multipart.getMediaType()), ValidateOkDto.class);

        assertFalse(response.getIsCompliant());
        assertEquals("1.0.0", response.getProfileVersion());
        assertEquals(ValidateOkDto.InformationPackageTypeEnum.DEPOSIT, response.getInformationPackageType());
        assertEquals(filename, response.getBagLocation());
        assertThat(response.getRuleViolations().size()).isEqualTo(1);
        assertThat(response.getRuleViolations().get(0).getViolation()).contains("not allowed");
        assertThat(response.getRuleViolations().get(0).getViolation()).contains("original-metadata.zip");
        assertThat(response.getRuleViolations().get(0).getRule()).isEqualTo("4.4");
    }

    @Test
    void validateFormData_should_not_throw_internal_server_error_on_incomplete_manifest() throws Exception {
        var filename = baseTestFolder + "/bags/bag-with-incomplete-manifest";

        var data = new ValidateCommandDto();
        data.setBagLocation(filename);
        data.setPackageType(ValidateCommandDto.PackageTypeEnum.DEPOSIT);
        var multipart = new FormDataMultiPart()
                .field("command", data, MediaType.APPLICATION_JSON_TYPE);

        var embargoResultJson = """
                {
                  "status": "OK",
                  "data": {
                    "message": "24"
                  }
                }""";
        var maxEmbargoDurationResult = new MockedDataverseResponse<DataMessage>(embargoResultJson, DataMessage.class);
        Mockito.when(dataverseService.getMaxEmbargoDurationInMonths())
                .thenReturn(maxEmbargoDurationResult);

        var response = EXT.target("/validate")
                .register(MultiPartFeature.class)
                .request()
                .post(Entity.entity(multipart, multipart.getMediaType()), ValidateOkDto.class);

        assertFalse(response.getIsCompliant());
        assertEquals("1.0.0", response.getProfileVersion());
        assertEquals(ValidateOkDto.InformationPackageTypeEnum.DEPOSIT, response.getInformationPackageType());
        assertEquals(filename, response.getBagLocation());
        assertThat(response.getRuleViolations().size()).isEqualTo(1);
        assertThat(response.getRuleViolations().get(0).getRule()).isEqualTo("1.1.1");
        assertThat(response.getRuleViolations().get(0).getViolation())
                .endsWith("original-metadata.zip] is in the payload directory but isn't listed in any manifest!");
    }

    @Test
    void validateZipFile_should_return_a_textual_representation_when_requested() throws Exception {
        var inputStream = Files.newInputStream(Path.of(baseTestFolder, "/zips/invalid-sha1.zip"));

        var embargoResultJson = """
                {
                  "status": "OK",
                  "data": {
                    "message": "24"
                  }
                }""";
        var maxEmbargoDurationResult = new MockedDataverseResponse<DataMessage>(embargoResultJson, DataMessage.class);
        Mockito.when(dataverseService.getMaxEmbargoDurationInMonths())
                .thenReturn(maxEmbargoDurationResult);

        var response = EXT.target("/validate")
                .request()
                .header("accept", "text/plain")
                .header("Authorization", basicUsernamePassword("user001", "user001"))
                .post(Entity.entity(inputStream, MediaType.valueOf("application/zip")), String.class);

        assertTrue(response.contains("Bag location:"));
        assertTrue(response.contains("Name:"));
        assertTrue(response.contains("Profile version:"));
        assertTrue(response.contains("Information package type:"));
        assertTrue(response.contains("Is compliant:"));
        assertTrue(response.contains("Rule violations:"));
    }

    @Test
    void validateFormData_with_invalid_path_should_return_400_error() {
        var data = new ValidateCommandDto();
        data.setBagLocation(baseTestFolder + "/some/non/existing/filename");
        data.setPackageType(ValidateCommandDto.PackageTypeEnum.DEPOSIT);

        var multipart = new FormDataMultiPart()
                .field("command", data, MediaType.APPLICATION_JSON_TYPE);

        try (var response = EXT.target("/validate")
                .register(MultiPartFeature.class)
                .request()
                .post(Entity.entity(multipart, multipart.getMediaType()), Response.class)) {

            assertEquals(400, response.getStatus());
        }
    }

    @Test
    void validateFormData_with_HasOrganizationalIdentifier_should_validate_if_results_from_dataverse_are_correct() throws Exception {
        var filename = baseTestFolder + "/bags/bag-with-is-version-of";

        var data = new ValidateCommandDto();
        data.setBagLocation(filename);
        data.setPackageType(ValidateCommandDto.PackageTypeEnum.DEPOSIT);

        var multipart = new FormDataMultiPart()
                .field("command", data, MediaType.APPLICATION_JSON_TYPE);

        var searchResultsJson = """
                {
                  "status": "OK",
                  "data": {
                    "q": "NBN:urn:nbn:nl:ui:13-025de6e2-bdcf-4622-b134-282b4c590f42",
                    "total_count": 1,
                    "start": 0,
                    "spelling_alternatives": {},
                    "items": [
                      {
                        "name": "Manual Test",
                        "type": "dataset",
                        "url": "https://doi.org/10.5072/FK2/QZZSST",
                        "global_id": "doi:10.5072/FK2/QZZSST"
                      }
                    ],
                    "count_in_response": 1
                  }
                }""";

        var latestVersionJson = """
                {
                  "status": "OK",
                  "data": {
                    "id": 2,
                    "identifier": "FK2/QZZSST",
                    "persistentUrl": "https://doi.org/10.5072/FK2/QZZSST",
                    "latestVersion": {
                      "id": 2,
                      "datasetId": 2,
                      "datasetPersistentId": "doi:10.5072/FK2/QZZSST",
                      "storageIdentifier": "file://10.5072/FK2/QZZSST",
                      "fileAccessRequest": false,
                      "metadataBlocks": {
                        "dansDataVaultMetadata": {
                          "displayName": "Data Vault Metadata",
                          "name": "dansDataVaultMetadata",
                          "fields": [
                            {
                              "typeName": "dansSwordToken",
                              "multiple": false,
                              "typeClass": "primitive",
                              "value": "urn:uuid:34632f71-11f8-48d8-9bf3-79551ad22b5e"
                            },
                            {
                              "typeName": "dansOtherId",
                              "multiple": false,
                              "typeClass": "primitive",
                              "value": "u1:organizational-identifier"
                            }
                          ]
                        }
                      }
                    }
                  }
                }""";

        var dataverseRoleAssignmentsJson = """
                {
                  "status": "OK",
                  "data": [
                    {
                      "id": 6,
                      "assignee": "@user001",
                      "roleId": 11,
                      "_roleAlias": "datasetcreator",
                      "definitionPointId": 2
                    }
                  ]
                }""";

        var datasetRoleAssignmentsJson = """
                {
                  "status": "OK",
                  "data": [
                    {
                      "id": 6,
                      "assignee": "@user001",
                      "roleId": 11,
                      "_roleAlias": "dataseteditor",
                      "definitionPointId": 2
                    }
                  ]
                }""";

        var embargoResultJson = """
                {
                  "status": "OK",
                  "data": {
                    "message": "24"
                  }
                }""";

        var searchResult = new MockedDataverseResponse<SearchResult>(searchResultsJson, SearchResult.class);
        var latestVersionResult = new MockedDataverseResponse<DatasetLatestVersion>(latestVersionJson, DatasetLatestVersion.class);
        var swordTokenResult = new MockedDataverseResponse<SearchResult>(searchResultsJson, SearchResult.class);
        var dataverseRoleAssignmentsResult = new MockedDataverseResponse<List<RoleAssignmentReadOnly>>(dataverseRoleAssignmentsJson, List.class, RoleAssignmentReadOnly.class);
        var datasetRoleAssignmentsResult = new MockedDataverseResponse<List<RoleAssignmentReadOnly>>(datasetRoleAssignmentsJson, List.class, RoleAssignmentReadOnly.class);
        var maxEmbargoDurationResult = new MockedDataverseResponse<DataMessage>(embargoResultJson, DataMessage.class);

        Mockito.when(dataverseService.getMaxEmbargoDurationInMonths())
                .thenReturn(maxEmbargoDurationResult);

        Mockito.when(dataverseService.getDataverseRoleAssignments(Mockito.anyString()))
                .thenReturn(dataverseRoleAssignmentsResult);

        Mockito.when(dataverseService.getDatasetRoleAssignments(Mockito.anyString()))
                .thenReturn(datasetRoleAssignmentsResult);

        Mockito.when(dataverseService.searchDatasetsByOrganizationalIdentifier(Mockito.anyString()))
                .thenReturn(searchResult);

        Mockito.when(dataverseService.getDataset(Mockito.anyString()))
                .thenReturn(latestVersionResult);

        Mockito.when(dataverseService.searchBySwordToken(Mockito.anyString()))
                .thenReturn(swordTokenResult);

        var response = EXT.target("/validate")
                .register(MultiPartFeature.class)
                .request()
                .post(Entity.entity(multipart, multipart.getMediaType()), ValidateOkDto.class);

        assertTrue(response.getIsCompliant());
        assertEquals("bag-with-is-version-of", response.getName());
    }

    @Test
    void validateFormData_with_HasOrganizationalIdentifier_should_not_validate_if_results_from_dataverse_are_incorrect() throws Exception {
        var filename = baseTestFolder + "/bags/bag-with-is-version-of";

        var data = new ValidateCommandDto();
        data.setBagLocation(filename);
        data.setPackageType(ValidateCommandDto.PackageTypeEnum.DEPOSIT);

        var multipart = new FormDataMultiPart()
                .field("command", data, MediaType.APPLICATION_JSON_TYPE);

        var searchResultsJson = """
                {
                  "status": "OK",
                  "data": {
                    "q": "NBN:urn:nbn:nl:ui:13-025de6e2-bdcf-4622-b134-282b4c590f42",
                    "total_count": 1,
                    "start": 0,
                    "spelling_alternatives": {},
                    "items": [
                      {
                        "name": "Manual Test",
                        "type": "dataset",
                        "url": "https://doi.org/10.5072/FK2/QZZSST",
                        "global_id": "doi:10.5072/FK2/QZZSST"
                      }
                    ],
                    "count_in_response": 1
                  }
                }""";

        // Violation will be caused by: "value": "urn:uuid:wrong-uuid"
        var latestVersionJson = """
                {
                  "status": "OK",
                  "data": {
                    "id": 2,
                    "identifier": "FK2/QZZSST",
                    "persistentUrl": "https://doi.org/10.5072/FK2/QZZSST",
                    "latestVersion": {
                      "id": 2,
                      "datasetId": 2,
                      "datasetPersistentId": "doi:10.5072/FK2/QZZSST",
                      "storageIdentifier": "file://10.5072/FK2/QZZSST",
                      "fileAccessRequest": false,
                      "metadataBlocks": {
                        "dansDataVaultMetadata": {
                          "displayName": "Data Vault Metadata",
                          "name": "dansDataVaultMetadata",
                          "fields": [
                            {
                              "typeName": "dansOtherId",
                              "multiple": false,
                              "typeClass": "primitive",
                              "value": "urn:uuid:wrong-uuid"
                            }
                          ]
                        }
                      }
                    }
                  }
                }""";

        var dataverseRoleAssignmentsJson = """
                {
                  "status": "OK",
                  "data": [
                    {
                      "id": 6,
                      "assignee": "@user001",
                      "roleId": 11,
                      "_roleAlias": "datasetcreator",
                      "definitionPointId": 2
                    }
                  ]
                }""";

        var datasetRoleAssignmentsJson = """
                {
                  "status": "OK",
                  "data": [
                    {
                      "id": 6,
                      "assignee": "@user001",
                      "roleId": 11,
                      "_roleAlias": "dataseteditor",
                      "definitionPointId": 2
                    }
                  ]
                }""";

        var embargoResultJson = """
                {
                  "status": "OK",
                  "data": {
                    "message": "24"
                  }
                }""";

        var searchResult = new MockedDataverseResponse<SearchResult>(searchResultsJson, SearchResult.class);
        var latestVersionResult = new MockedDataverseResponse<DatasetLatestVersion>(latestVersionJson, DatasetLatestVersion.class);
        var swordTokenResult = new MockedDataverseResponse<SearchResult>(searchResultsJson, SearchResult.class);
        var dataverseRoleAssignmentsResult = new MockedDataverseResponse<List<RoleAssignmentReadOnly>>(dataverseRoleAssignmentsJson, List.class, RoleAssignmentReadOnly.class);
        var datasetRoleAssignmentsResult = new MockedDataverseResponse<List<RoleAssignmentReadOnly>>(datasetRoleAssignmentsJson, List.class, RoleAssignmentReadOnly.class);
        var maxEmbargoDurationResult = new MockedDataverseResponse<DataMessage>(embargoResultJson, DataMessage.class);

        Mockito.when(dataverseService.getMaxEmbargoDurationInMonths())
                .thenReturn(maxEmbargoDurationResult);

        Mockito.when(dataverseService.searchDatasetsByOrganizationalIdentifier(Mockito.anyString()))
                .thenReturn(searchResult);

        Mockito.when(dataverseService.getDataset(Mockito.anyString()))
                .thenReturn(latestVersionResult);

        Mockito.when(dataverseService.searchBySwordToken(Mockito.anyString()))
                .thenReturn(swordTokenResult);

        Mockito.when(dataverseService.getDataverseRoleAssignments(Mockito.anyString()))
                .thenReturn(dataverseRoleAssignmentsResult);

        Mockito.when(dataverseService.getDatasetRoleAssignments(Mockito.anyString()))
                .thenReturn(datasetRoleAssignmentsResult);

        var response = EXT.target("/validate")
                .register(MultiPartFeature.class)
                .request()
                .post(Entity.entity(multipart, multipart.getMediaType()), ValidateOkDto.class);

        var failed = response.getRuleViolations().stream()
                .map(ValidateOkRuleViolationsInnerDto::getRule).collect(Collectors.toSet());

        assertEquals(Set.of("4.1(b)"), failed);
        assertFalse(response.getIsCompliant());
        assertEquals("bag-with-is-version-of", response.getName());
    }

    @Test
    void validateFormData_should_yield_violation_errors_if_swordToken_does_not_match() throws Exception {
        var filename = baseTestFolder + "/bags/bag-with-is-version-of";

        var data = new ValidateCommandDto();
        data.setBagLocation(filename);
        data.setPackageType(ValidateCommandDto.PackageTypeEnum.DEPOSIT);

        var multipart = new FormDataMultiPart()
                .field("command", data, MediaType.APPLICATION_JSON_TYPE);

        var searchResultsJson = """
                {
                  "status": "OK",
                  "data": {
                    "q": "NBN:urn:nbn:nl:ui:13-025de6e2-bdcf-4622-b134-282b4c590f42",
                    "total_count": 1,
                    "start": 0,
                    "spelling_alternatives": {},
                    "items": [
                      {
                        "name": "Manual Test",
                        "type": "dataset",
                        "url": "https://doi.org/10.5072/FK2/QZZSST",
                        "global_id": "doi:10.5072/FK2/QZZSST"
                      }
                    ],
                    "count_in_response": 1
                  }
                }""";

        // returns 0 items, causing the rule to be violated
        var swordTokenJson = """
                {
                  "status": "OK",
                  "data": {
                    "q": "NBN:urn:nbn:nl:ui:13-025de6e2-bdcf-4622-b134-282b4c590f42",
                    "total_count": 1,
                    "start": 0,
                    "spelling_alternatives": {},
                    "items": [
                    ],
                    "count_in_response": 0
                  }
                }""";

        var latestVersionJson = """
                {
                  "status": "OK",
                  "data": {
                    "id": 2,
                    "identifier": "FK2/QZZSST",
                    "persistentUrl": "https://doi.org/10.5072/FK2/QZZSST",
                    "latestVersion": {
                      "id": 2,
                      "datasetId": 2,
                      "datasetPersistentId": "doi:10.5072/FK2/QZZSST",
                      "storageIdentifier": "file://10.5072/FK2/QZZSST",
                      "fileAccessRequest": false,
                      "metadataBlocks": {
                        "dansDataVaultMetadata": {
                          "displayName": "Data Vault Metadata",
                          "name": "dansDataVaultMetadata",
                          "fields": [
                            {
                              "typeName": "dansSwordToken",
                              "multiple": false,
                              "typeClass": "primitive",
                              "value": "urn:uuid:34632f71-11f8-48d8-9bf3-79551ad22b5e"
                            }
                          ]
                        }
                      }
                    }
                  }
                }""";

        var embargoResultJson = """
                {
                  "status": "OK",
                  "data": {
                    "message": "24"
                  }
                }""";

        var searchResult = new MockedDataverseResponse<SearchResult>(searchResultsJson, SearchResult.class);
        var latestVersionResult = new MockedDataverseResponse<DatasetLatestVersion>(latestVersionJson, DatasetLatestVersion.class);
        var swordTokenResult = new MockedDataverseResponse<SearchResult>(swordTokenJson, SearchResult.class);
        var maxEmbargoDurationResult = new MockedDataverseResponse<DataMessage>(embargoResultJson, DataMessage.class);

        Mockito.when(dataverseService.getMaxEmbargoDurationInMonths())
                .thenReturn(maxEmbargoDurationResult);

        Mockito.when(dataverseService.searchDatasetsByOrganizationalIdentifier(Mockito.anyString()))
                .thenReturn(searchResult);

        Mockito.when(dataverseService.getDataset(Mockito.anyString()))
                .thenReturn(latestVersionResult);

        Mockito.when(dataverseService.searchBySwordToken(Mockito.anyString()))
                .thenReturn(swordTokenResult);

        var response = EXT.target("/validate")
                .register(MultiPartFeature.class)
                .request()
                .post(Entity.entity(multipart, multipart.getMediaType()), ValidateOkDto.class);

        var failed = response.getRuleViolations().stream()
                .map(ValidateOkRuleViolationsInnerDto::getRule).collect(Collectors.toSet());

        assertEquals(Set.of("4.1(a)", "4.1(b)"), failed);
        assertFalse(response.getIsCompliant());
        assertEquals("bag-with-is-version-of", response.getName());
    }
}
