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

import lombok.AllArgsConstructor;
import nl.knaw.dans.validatedansbag.core.service.FileService;
import nl.knaw.dans.validatedansbag.core.service.RuleEngineService;

import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;

@AllArgsConstructor
public class ValidateZipApiResource implements ValidateZipApi {
    private final RuleEngineService ruleEngineService;
    private final FileService fileService;

    @Override
    public Response validateZipPost(File body) {
        try (var inputStream = new FileInputStream(body)) {
            var dir = fileService.extractZipFile(inputStream);
            var bagDir = fileService.getFirstDirectory(dir);
            if (bagDir.isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST).entity("No bag directory found in zip file").build();
            }
            var result = ruleEngineService.validateBag(bagDir.get(), "ZIP");
            return Response.ok(result).build();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
