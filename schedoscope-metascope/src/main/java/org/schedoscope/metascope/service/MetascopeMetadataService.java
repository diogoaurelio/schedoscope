/**
 * Copyright 2017 Otto (GmbH & Co KG)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.metascope.service;

import org.schedoscope.metascope.model.MetascopeMetadata;
import org.schedoscope.metascope.repository.MetascopeMetadataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MetascopeMetadataService {

  @Autowired
  private MetascopeMetadataRepository metascopeMetadataRepository;

  public String getMetadataValue(String key) {
    MetascopeMetadata metadata = metascopeMetadataRepository.findOne(key);
    if (metadata == null) {
      return null;
    }
    return metadata.getMetadataValue();
  }

  public void save(String key, String value) {
    metascopeMetadataRepository.save(new MetascopeMetadata(key, value));
  }

}
