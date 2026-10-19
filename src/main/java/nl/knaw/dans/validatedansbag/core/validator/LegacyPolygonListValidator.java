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
package nl.knaw.dans.validatedansbag.core.validator;

import lombok.AllArgsConstructor;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;

@AllArgsConstructor
public class LegacyPolygonListValidator implements PolygonListValidator {
    private final PolygonListValidator delegate;

    @Override
    public PolygonValidationResult validatePolygonList(String polygons) {
        var result = delegate.validatePolygonList(polygons);
        if (!result.isValid()) {
            return result;
        }
        var parts = polygons.trim().split("\\s+");
        var points = new HashSet<List<BigDecimal>>();
        try {
            for (int i = 0; i < parts.length; i += 2) {
                points.add(List.of(new BigDecimal(parts[i]).stripTrailingZeros(), new BigDecimal(parts[i + 1]).stripTrailingZeros()));
            }
        }
        catch (NumberFormatException e) {
            return PolygonValidationResult.invalid("Polygon coordinates must be numeric");
        }
        return points.size() >= 3 ? result : PolygonValidationResult.invalid("Polygon must contain at least three different coordinate pairs");
    }
}
