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
package nl.knaw.dans.validatedansbag.core.rules;

import lombok.AllArgsConstructor;
import nl.knaw.dans.lib.util.XmlSchemaValidator;
import nl.knaw.dans.lib.util.ruleengine.NumberedRule;
import nl.knaw.dans.validatedansbag.core.service.BagItMetadataReader;
import nl.knaw.dans.validatedansbag.core.service.DataverseService;
import nl.knaw.dans.validatedansbag.core.service.FileService;
import nl.knaw.dans.validatedansbag.core.service.FilesXmlService;
import nl.knaw.dans.validatedansbag.core.service.OriginalFilepathsService;
import nl.knaw.dans.validatedansbag.core.service.VaultCatalogClient;
import nl.knaw.dans.validatedansbag.core.service.XmlReader;
import nl.knaw.dans.validatedansbag.core.validator.IdentifierValidator;
import nl.knaw.dans.validatedansbag.core.validator.LicenseValidator;
import nl.knaw.dans.validatedansbag.core.validator.OrganizationIdentifierPrefixValidator;
import nl.knaw.dans.validatedansbag.core.validator.PolygonListValidator;
import org.apache.commons.collections4.ListUtils;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

@AllArgsConstructor
public class RuleSets {
    private static final Path metadataPath = Path.of("metadata");
    private static final Path payloadPath = Path.of("data");
    private static final Path metadataFilesPath = Path.of("metadata/files.xml");

    private static final Path datasetPath = Path.of("metadata/dataset.xml");

    private final DataverseService dataverseService;
    private final FileService fileService;
    private final FilesXmlService filesXmlService;
    private final OriginalFilepathsService originalFilepathService;
    private final XmlReader xmlReader;

    private final BagItMetadataReader bagItMetadataReader;

    private final XmlSchemaValidator xmlSchemaValidator;

    private final LicenseValidator licenseValidator;

    private final IdentifierValidator identifierValidator;

    private final PolygonListValidator polygonListValidator;

    private final OrganizationIdentifierPrefixValidator organizationIdentifierPrefixValidator;

    private final VaultCatalogClient vaultCatalogClient;

    private final Map<URI, Set<URI>> schemeUriToValidTermUris;

    private final Map<URI, Set<String>> schemeUriToValidCodes;

    public List<NumberedRule> getDataStationSet() {
        return ListUtils.union(getCommonRules(), getDataStationOnlyRules());
    }

    public List<NumberedRule> getVaasSet() {
        return ListUtils.union(getCommonRules(), getVaasOnlyRules());
    }

    private List<NumberedRule> getCommonRules() {
        return List.of(
            // 1 BagIt related¶

            // 1.1 Validity¶
            new NumberedRule("1.1.1", new BagIsValid(bagItMetadataReader)),

            // 1.2 bag-info.txt
            new NumberedRule("1.2.1", new BagInfoExistsAndIsWellformed(bagItMetadataReader, fileService)),
            // 1.2.2 MAY/SHOULD-requirement, so not validated
            new NumberedRule("1.2.3(a)", new BagInfoContainsAtMostOneOf("Is-Version-Of", bagItMetadataReader), List.of("1.2.1")),
            new NumberedRule("1.2.3(b)", new BagInfoIsVersionOfIsValidUrnUuid(bagItMetadataReader), List.of("1.2.3(a)")),
            new NumberedRule("1.2.4(a)", new BagInfoContainsAtMostOneOf("Has-Organizational-Identifier", bagItMetadataReader), List.of("1.2.1")),
            new NumberedRule("1.2.4(b)", new BagInfoContainsAtMostOneOf("Has-Organizational-Identifier-Version", bagItMetadataReader), List.of("1.2.4(a)")),
            new NumberedRule("1.2.4(c)", new BagInfoOrganizationalIdentifierPrefixIsValid(bagItMetadataReader, organizationIdentifierPrefixValidator), List.of("1.2.4(a)")),

            // 1.3. Manifests
            new NumberedRule("1.3.1", new BagHasOtherManifestsThanOnlyMd5(bagItMetadataReader), List.of("1.1.1")),

            // 2 Structural
            new NumberedRule("2.1", new BagContainsDir(metadataPath, fileService), List.of("1.1.1")),
            new NumberedRule("2.2(a)", new BagContainsFile(metadataPath.resolve("dataset.xml"), fileService), List.of("2.1")),
            new NumberedRule("2.2(b)", new BagContainsFile(metadataPath.resolve("files.xml"), fileService), List.of("2.1")),
            new NumberedRule("2.3", new BagDirContainsNothingElseThan(metadataPath, new String[] {
                "dataset.xml",
                "files.xml"
            }, fileService), List.of("2.1")),
            // 2.4 is covered by 3.3.1

            // 3 Metadata requirements¶

            // 3.1 metadata/dataset.xml¶
            new NumberedRule("3.1.1", new BagFileConformsToXmlSchema(datasetPath, fileService,"dataset.xml", xmlSchemaValidator), List.of("1.1.1", "2.2(a)")),
            new NumberedRule("3.1.2", new DatasetXmlContainsExactlyOneDctermsLicenseWithXsiTypeUri(xmlReader, licenseValidator), List.of("3.1.1")),

            new NumberedRule("3.1.3(a)", new DatasetXmlDaisAreValid(xmlReader, identifierValidator), List.of("3.1.1")),
            new NumberedRule("3.1.3(b)", new DatasetXmlIsnisAreValid(xmlReader, identifierValidator), List.of("3.1.1")),
            new NumberedRule("3.1.3(c)", new DatasetXmlOrcidsAreValid(xmlReader, identifierValidator), List.of("3.1.1")),
            new NumberedRule("3.1.4", new DatasetXmlGmlPolygonPosListIsWellFormed(xmlReader, polygonListValidator), List.of("3.1.1")),
            new NumberedRule("3.1.5", new DatasetXmlGmlPolygonsInSameMultiSurfaceHaveSameSrsName(xmlReader), List.of("3.1.1")),
            new NumberedRule("3.1.6", new DatasetXmlGmlPointsHaveAtLeastTwoValues(xmlReader), List.of("3.1.1")),
            new NumberedRule("3.1.7", new DatasetXmlArchisIdentifiersHaveAtMost10Characters(xmlReader), List.of("3.1.1")),
            new NumberedRule("3.1.8", new DatasetXmlAllUrlsAreValid(xmlReader), List.of("3.1.1")),

            new NumberedRule("3.1.9", new DatasetXmlHasRightsHolderInElement(xmlReader), List.of("3.1.1")),
            new NumberedRule("3.1.10", new DatasetXmlDoesNotHaveRightHolderInAuthorRole(xmlReader), List.of("3.1.1")),
            new NumberedRule("3.1.11", new DatasetXmlExactlyOneOfValueUriAndValueCode(xmlReader), List.of("3.1.1")),
            new NumberedRule("3.1.12(a)", new DatasetXmlValueUrisAreValid(xmlReader, schemeUriToValidTermUris), List.of("3.1.1")),
            new NumberedRule("3.1.12(b)", new DatasetXmlValueCodesAreValid(xmlReader, schemeUriToValidCodes), List.of("3.1.1")),

            // 3.2 metadata/files.xml
            new NumberedRule("3.2.1", new BagFileConformsToXmlSchema(metadataFilesPath, fileService,"files.xml", xmlSchemaValidator), List.of("1.1.1", "2.2(b)")),
            new NumberedRule("3.2.2", new FilesXmlFilePathAttributesContainLocalBagPathAndNonPayloadFilesAreNotDescribed(fileService, filesXmlService, originalFilepathService), List.of("3.2.1")),
            new NumberedRule("3.2.3", new FilesXmlNoDuplicateFilesAndEveryPayloadFileIsDescribed(filesXmlService, fileService, originalFilepathService), List.of("3.2.1")),

            // 3.3 original-filepaths.txt
            new NumberedRule("3.3.1", new OptionalBagFileIsUtf8Decodable(Path.of("original-filepaths.txt"), fileService), List.of("1.1.1")),
            new NumberedRule("3.3.2", new OptionalOriginalFilePathsIsComplete(originalFilepathService, fileService, filesXmlService), List.of("3.3.1")));
    }

    private List<NumberedRule> getDataStationOnlyRules() {
        return List.of(
            new NumberedRule("4.1(a)", new BagInfoIsVersionOfPointsToExistingDatasetInDataverse(dataverseService, bagItMetadataReader), List.of("1.2.3(a)")),
            new NumberedRule("4.1(b)", new BagInfoOrganizationalIdentifierExistsInDataset(dataverseService, bagItMetadataReader), List.of("1.2.3(a)", "1.2.4(a)")),
            new NumberedRule("4.2", new DatasetXmlLicenseAllowedByDatastation(xmlReader, licenseValidator), List.of("3.1.2")),
            new NumberedRule("4.3", new DatasetXmlEmbargoPeriodWithinLimits(dataverseService, xmlReader), List.of("3.1.1")),
            new NumberedRule("4.4", new BagDirDoesNotContain(payloadPath, new String[] {
                "original-metadata.zip"
            }, fileService), List.of("1.1.1"))
        );
    }

    private List<NumberedRule> getVaasOnlyRules() {
        // 5 Vault as a Service context requirements
        return List.of(
            // TODO: 5.1
            new NumberedRule("5.1", new BagInfoIsVersionOfPointsToExistingDatasetInVaultCatalog(vaultCatalogClient, bagItMetadataReader), List.of("3.1.1")),
            new NumberedRule("5.2(a)", new DatasetXmlContainsAtMostOneIdentifierWithIdTypeDoi(xmlReader), List.of("3.1.1")),
            new NumberedRule("5.2(b)", new DatasetXmlDoisAreValid(xmlReader), List.of("5.2(a)"))
        );
    }

}

