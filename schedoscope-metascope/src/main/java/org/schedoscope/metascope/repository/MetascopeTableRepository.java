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
package org.schedoscope.metascope.repository;

import org.schedoscope.metascope.model.MetascopeCategoryObject;
import org.schedoscope.metascope.model.MetascopeComment;
import org.schedoscope.metascope.model.MetascopeTable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Set;

public interface MetascopeTableRepository extends CrudRepository<MetascopeTable, String> {

  @Query("SELECT t FROM MetascopeTable t WHERE :co MEMBER OF t.categoryObjects")
  public List<MetascopeTable> findByCategoryObject(@Param(value = "co") MetascopeCategoryObject co);

  @Query("SELECT t FROM MetascopeTable t WHERE :commentEntity MEMBER OF t.comments")
  public MetascopeTable findByComment(@Param(value = "commentEntity") MetascopeComment commentEntity);

  @Query("SELECT distinct(t.personResponsible) FROM MetascopeTable t where t.personResponsible is not null")
  public Set<String> getAllOwner();

  public List<MetascopeTable> findTop5ByOrderByViewCountDesc();

  @Query("SELECT t.fqdn FROM MetascopeTable t")
  public List<String> getAllTablesNames();

}
