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
package nl.knaw.dans.validatedansbag.core.service;

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.validatedansbag.core.BagNotFoundException;
import nl.knaw.dans.validatedansbag.core.engine.DepositType;
import nl.knaw.dans.validatedansbag.core.engine.NumberedRule;
import nl.knaw.dans.validatedansbag.core.engine.RuleEngine;
import nl.knaw.dans.validatedansbag.core.engine.RuleEngineConfigurationException;
import nl.knaw.dans.validatedansbag.core.engine.RuleValidationResult;
import nl.knaw.dans.validatedansbag.core.rules.*;
import nl.knaw.dans.validatedansbag.core.validator.LicenseValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

@Slf4j
public class RuleEngineServiceImpl implements RuleEngineService {

    private final RuleEngine ruleEngine;
    private final FileService fileService;
    private final NumberedRule[] defaultRules;
    private final Path datasetPath = Path.of("metadata/dataset.xml");
    private final Path metadataPath = Path.of("metadata");
    private final Path payloadPath = Path.of("data");
    private final Path metadataFilesPath = Path.of("metadata/files.xml");

    public RuleEngineServiceImpl(RuleEngine ruleEngine, BagRules bagRules, XmlRules xmlRules, FilesXmlRules filesXmlRules, FileService fileService, DatastationRules datastationRules, VaasRules vaasRules, FilesXmlService filesXmlService, OriginalFilepathsService originalFilepathService, XmlReader xmlReader, LicenseValidator licenseValidator) {
        this.ruleEngine = ruleEngine;
        this.fileService = fileService;
        var bagItMetadataReader = new BagItMetadataReaderImpl();

        // validity
        this.defaultRules = new NumberedRule[]{
                new NumberedRule("1.1.1", new BagIsValid(bagItMetadataReader)),

                // bag-info.txt
                new NumberedRule("1.2.1", new BagInfoExistsAndIsWellformed(bagItMetadataReader, fileService)),
                new NumberedRule("1.2.2(a)", new BagInfoContainsExactlyOneOf("Created", bagItMetadataReader), List.of("1.2.1")),
                new NumberedRule("1.2.2(b)", new BagInfoCreatedElementIsIso8601Date(bagItMetadataReader), List.of("1.2.2(a)")),
                new NumberedRule("1.2.3(a)", new BagInfoContainsAtMostOneOf("Is-Version-Of", bagItMetadataReader), List.of("1.2.1")),
                new NumberedRule("1.2.3(b)", new BagInfoIsVersionOfIsValidUrnUuid(bagItMetadataReader), List.of("1.2.3(a)")),
                new NumberedRule("1.2.4(a)", new BagInfoContainsAtMostOneOf("Has-Organizational-Identifier", bagItMetadataReader), List.of("1.2.1")),
                new NumberedRule("1.2.4(b)", new BagInfoContainsAtMostOneOf("Has-Organizational-Identifier-Version", bagItMetadataReader), List.of("1.2.4(a)")),

                // manifests
                new NumberedRule("1.3.1", bagRules.containsNotJustMD5Manifest(), List.of("1.1.1")),

                // Structural
                new NumberedRule("2.1", new ContainsDir(metadataPath, fileService), List.of("1.1.1")),
                new NumberedRule("2.2(a)", new ContainsFile(metadataPath.resolve("dataset.xml"), fileService), List.of("2.1")),
                new NumberedRule("2.2(b)", new ContainsFile(metadataPath.resolve("files.xml"), fileService), List.of("2.1")),

                // this also covers 2.3 and 2.4 for MIGRATION status deposits
                new NumberedRule("2.2-MIGRATION", new ContainsNothingElseThan(metadataPath, new String[]{
                        "dataset.xml",
                        "files.xml",
                        "provenance.xml",
                        "amd.xml",
                        "emd.xml",
                        "original",
                        "original/dataset.xml",
                        "original/files.xml",
                        "depositor-info",
                        "depositor-info/agreements.xml",
                        "depositor-info/depositor-agreement.pdf",
                        "depositor-info/depositor-agreement.txt",
                        "depositor-info/message-from-depositor.txt",
                        "license.html",
                        "license.txt",
                        "license.pdf"
                }, fileService), DepositType.MIGRATION, List.of("2.1")),

                new NumberedRule("2.3", new ContainsNothingElseThan(metadataPath, new String[]{
                        "dataset.xml",
                        "files.xml"
                }, fileService), DepositType.DEPOSIT, List.of("2.1")),

                new NumberedRule("2.5", new MustNotContain(payloadPath, new String[]{
                        "original-metadata.zip"
                }, fileService), List.of("1.1.1")),

                // metadata/dataset.xml
                new NumberedRule("3.1.1", xmlRules.xmlFileConformsToSchema(datasetPath, "dataset.xml"), List.of("1.1.1", "2.2(a)")),
                new NumberedRule("3.1.2", new DdmMustContainExactlyOneDctermsLicenseWithXsiTypeUri(xmlReader, licenseValidator), List.of("3.1.1")),

                new NumberedRule("3.1.4(a)", bagRules.ddmDaisAreValid(), List.of("3.1.1")),
                new NumberedRule("3.1.4(b)", bagRules.ddmIsnisAreValid(), List.of("3.1.1")),
                new NumberedRule("3.1.4(c)", bagRules.ddmOrcidsAreValid(), List.of("3.1.1")),
                new NumberedRule("3.1.5", bagRules.ddmGmlPolygonPosListIsWellFormed(), List.of("3.1.1")),
                new NumberedRule("3.1.6", bagRules.polygonsInSameMultiSurfaceHaveSameSrsName(), List.of("3.1.1")),
                new NumberedRule("3.1.7", bagRules.pointsHaveAtLeastTwoValues(), List.of("3.1.1")),
                new NumberedRule("3.1.8", bagRules.archisIdentifiersHaveAtMost10Characters(), List.of("3.1.1")),
                new NumberedRule("3.1.9", bagRules.allUrlsAreValid(), List.of("3.1.1")),

                new NumberedRule("3.1.10", bagRules.ddmMustHaveRightsHolderDeposit(), DepositType.DEPOSIT, List.of("3.1.1")),
                new NumberedRule("3.1.10-MIGRATION", bagRules.ddmMustHaveRightsHolderDeposit(), DepositType.MIGRATION, List.of("3.1.1")),
                new NumberedRule("3.1.11", bagRules.ddmMustNotHaveRightsHolderRole(), DepositType.DEPOSIT, List.of("3.1.1")),

                new NumberedRule("3.2.1", xmlRules.xmlFileConformsToSchema(metadataFilesPath, "files.xml"), List.of("3.1.1")),
                new NumberedRule("3.2.2", filesXmlRules.filesXmlFilePathAttributesContainLocalBagPathAndNonPayloadFilesAreNotDescribed(), List.of("2.2(b)")),
                new NumberedRule("3.2.3", filesXmlRules.filesXmlNoDuplicateFilesAndEveryPayloadFileIsDescribed(), List.of("2.2(b)")),

                // original-filepaths.txt
                new NumberedRule("3.3.1", new OptionalFileIsUtf8Decodable(Path.of("original-filepaths.txt"), fileService), List.of("1.1.1")),
                new NumberedRule("3.3.2", new IsOriginalFilepathsFileComplete(originalFilepathService, fileService, filesXmlService), List.of("3.3.1")),

                // agreements.xml
                new NumberedRule("3.4.1-MIGRATION", xmlRules.xmlFileIfExistsConformsToSchema(Path.of("metadata/depositor-info/agreements.xml"), "agreements.xml"), DepositType.MIGRATION),

                // amd.xml
                new NumberedRule("3.4.2-MIGRATION", xmlRules.xmlFileIfExistsConformsToSchema(Path.of("metadata/amd.xml"), "amd.xml"), DepositType.MIGRATION),

                // emd.xml
                new NumberedRule("3.4.3-MIGRATION", xmlRules.xmlFileIfExistsConformsToSchema(Path.of("metadata/emd.xml"), "emd.xml"), DepositType.MIGRATION),

                // provenance.xml
                new NumberedRule("3.4.4-MIGRATION", xmlRules.xmlFileIfExistsConformsToSchema(Path.of("metadata/provenance.xml"), "provenance.xml"), DepositType.MIGRATION),

                new NumberedRule("4.1", bagRules.organizationalIdentifierPrefixIsValid(), DepositType.DEPOSIT, List.of("1.2.4(a)")),
                new NumberedRule("4.2(a)", datastationRules.bagExistsInDatastation(), DepositType.DEPOSIT, List.of("4.1")),
                new NumberedRule("4.2(b)", datastationRules.organizationalIdentifierExistsInDataset(), DepositType.DEPOSIT, List.of("1.2.3(a)")),
                new NumberedRule("4.3", datastationRules.licenseExistsInDatastation(), DepositType.DEPOSIT, List.of("3.1.2")),
                new NumberedRule("4.4", datastationRules.embargoPeriodWithinLimits(), DepositType.DEPOSIT),


                new NumberedRule("5.2", vaasRules.ddmDoiIdentifiersAreValid(), List.of("3.1.1")),

        };

        this.validateRuleConfiguration();
    }

    @Override
    public List<RuleValidationResult> validateBag(Path path, DepositType depositType) throws Exception {
        log.info("Validating bag on path '{}', deposit type is {}", path, depositType);

        if (!fileService.isReadable(path)) {
            log.warn("Path {} could not not be found or is not readable", path);
            throw new BagNotFoundException(String.format("Bag on path '%s' could not be found or read", path));
        }

        return ruleEngine.validateRules(path, this.defaultRules, depositType);
    }

    public void validateRuleConfiguration() {
        try {
            this.ruleEngine.validateRuleConfiguration(this.defaultRules);
        } catch (RuleEngineConfigurationException e) {
            throw new RuntimeException("Rule configuration is not valid", e);
        }
    }
}
