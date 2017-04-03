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

import org.schedoscope.metascope.model.MetascopeComment;
import org.schedoscope.metascope.model.MetascopeField;
import org.schedoscope.metascope.repository.MetascopeFieldRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class MetascopeFieldService {

    @Autowired
    private MetascopeFieldRepository metascopeFieldRepository;

    public List<String> findDistinctParameters() {
        List<String> list = new ArrayList<String>();
        List<Object[]> parameters = metascopeFieldRepository.findDistinctParameters();
        for (Object[] field : parameters) {
            list.add((String) field[0]);
        }
        return list;
    }

    public MetascopeField findById(String id) {
        return metascopeFieldRepository.findOne(id);
    }

    public MetascopeField findByComment(MetascopeComment commentEntity) {
        return metascopeFieldRepository.findByComment(commentEntity);
    }

    public void setMetascopeFieldRepository(MetascopeFieldRepository metascopeFieldRepository) {
        this.metascopeFieldRepository = metascopeFieldRepository;
    }
}
