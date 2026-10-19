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

import nl.knaw.dans.lib.util.ruleengine.RuleEngineImpl;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RuleEngineServiceImplTest {
    @Test
    void reportsSelectedProfileVersion() throws Exception {
        var fileService = Mockito.mock(FileService.class);
        var path = Path.of("bag");
        Mockito.when(fileService.isReadable(path)).thenReturn(true);

        var legacy = new RuleEngineServiceImpl(new RuleEngineImpl(), fileService, List.of(), "0.0.0");
        assertThat(legacy.validateBag(path, "bag").getProfileVersion()).isEqualTo("0.0.0");

        var current = new RuleEngineServiceImpl(new RuleEngineImpl(), fileService, List.of());
        assertThat(current.validateBag(path, "bag").getProfileVersion()).isEqualTo("1.2.0");
    }
}
