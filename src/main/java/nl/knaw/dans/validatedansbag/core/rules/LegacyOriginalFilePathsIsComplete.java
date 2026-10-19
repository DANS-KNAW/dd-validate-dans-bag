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

import nl.knaw.dans.lib.util.ruleengine.RuleResult;
import nl.knaw.dans.validatedansbag.core.service.FileService;
import nl.knaw.dans.validatedansbag.core.service.FilesXmlService;
import nl.knaw.dans.validatedansbag.core.service.OriginalFilepathsService;

import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.HashSet;

public class LegacyOriginalFilePathsIsComplete extends OptionalOriginalFilePathsIsComplete {
    private final FileService fileService;

    public LegacyOriginalFilePathsIsComplete(OriginalFilepathsService originalFilepathsService, FileService fileService, FilesXmlService filesXmlService) {
        super(originalFilepathsService, fileService, filesXmlService);
        this.fileService = fileService;
    }

    @Override
    public RuleResult validate(Path path) throws Exception {
        var mappingFile = path.resolve("original-filepaths.txt");
        if (!fileService.exists(mappingFile)) {
            return RuleResult.ok();
        }

        var physical = new HashSet<Path>();
        var original = new HashSet<Path>();
        var lines = new String(fileService.readFileContents(mappingFile), StandardCharsets.UTF_8).lines().toList();
        for (var line : lines) {
            var parts = line.split("\\s+", 2);
            if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
                return RuleResult.error("original-filepaths.txt: expected physical-path whitespace original-path");
            }
            try {
                if (!physical.add(Path.of(parts[0]).normalize()) || !original.add(Path.of(parts[1]).normalize())) {
                    return RuleResult.error("original-filepaths.txt: physical and original paths must be one-to-one");
                }
            }
            catch (InvalidPathException e) {
                return RuleResult.error("original-filepaths.txt: invalid path", e);
            }
        }
        return super.validate(path);
    }
}
