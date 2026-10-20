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

package nl.knaw.dans.validatedansbag.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.dropwizard.core.Configuration;
import lombok.Data;
import lombok.EqualsAndHashCode;
import nl.knaw.dans.lib.util.DataverseClientFactory;

import javax.validation.Valid;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotNull;

@Data 
@EqualsAndHashCode(callSuper = true)
public class DdValidateDansBagConfiguration extends Configuration {

    private boolean easyLegacyMode;

    @JsonDeserialize(using = EmptyObjectAsNullDeserializer.class)
    private DataverseClientFactory dataverse;

    @JsonDeserialize(using = EmptyObjectAsNullDeserializer.class)
    private VaultCatalogConfig vaultCatalog;

    @Valid
    public DataverseClientFactory getDataverse() {
        return easyLegacyMode ? null : dataverse;
    }

    @Valid
    public VaultCatalogConfig getVaultCatalog() {
        return easyLegacyMode ? null : vaultCatalog;
    }

    @JsonIgnore
    @AssertTrue(message = "Exactly one of dataverse and vaultCatalog must be specified unless easyLegacyMode is true")
    public boolean isBackendConfigurationValid() {
        return easyLegacyMode || (dataverse != null) != (vaultCatalog != null);
    }


    @Valid
    @NotNull
    private ValidationConfig validation;
}
