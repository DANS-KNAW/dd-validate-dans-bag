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

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.util.ruleengine.BagValidatorRule;
import nl.knaw.dans.lib.util.ruleengine.RuleResult;
import nl.knaw.dans.validatedansbag.core.service.BagItMetadataReader;
import nl.knaw.dans.validatedansbag.core.service.VaultCatalogClient;

import java.nio.file.Path;

@Slf4j
public class BagInfoIsVersionOfPointsToExistingDatasetInVaultCatalog implements BagValidatorRule {
    private final VaultCatalogClient vaultCatalogClient;
    private final BagItMetadataReader bagItMetadataReader;

    public BagInfoIsVersionOfPointsToExistingDatasetInVaultCatalog(VaultCatalogClient vaultCatalogClient, BagItMetadataReader bagItMetadataReader) {
        this.vaultCatalogClient = vaultCatalogClient;
        this.bagItMetadataReader = bagItMetadataReader;
    }

    @Override
    public RuleResult validate(Path path) throws Exception {
        if (this.vaultCatalogClient == null) {
            throw new IllegalStateException("Vault catalog rule called, but vault service is not configured");
        }

        var isVersionOf = bagItMetadataReader.getSingleField(path, "Is-Version-Of");

        log.debug("Using Is-Version-Of value '{}' to find a matching dataset", isVersionOf);

        if (isVersionOf != null) {
            var swordToken = convertToSwordToken(isVersionOf);
            var dataset = vaultCatalogClient.findDatasetBySwordToken(swordToken);

            if (dataset.isEmpty()) {
                log.debug("Dataset with sword token '{}' not found", swordToken);
                // no result means it does not exist
                return RuleResult.error(String.format("If 'Is-Version-Of' is specified, it must be a valid SWORD token in the vault catalog; no tokens were found: %s", isVersionOf));
            }
            else {
                log.debug("Dataset with sword token '{}': {}", swordToken, dataset.get());
                return RuleResult.ok();
            }
        }

        return RuleResult.skipDependencies();
    }

    private String convertToSwordToken(String isVersionOf) {
        if (isVersionOf.startsWith("sword:")) {
            return isVersionOf;
        }
        else if (isVersionOf.startsWith("urn:uuid:")) {
            return "sword:" + isVersionOf.substring("urn:uuid:".length());
        }
        else {
            throw new IllegalArgumentException("Is-Version-Of value must start with 'sword:' or 'urn:uuid:'");
        }
    }

}
