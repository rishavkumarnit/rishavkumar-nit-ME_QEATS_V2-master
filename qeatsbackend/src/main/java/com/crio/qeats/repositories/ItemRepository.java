
package com.crio.qeats.repositories;

import com.crio.qeats.models.ItemEntity;
import java.util.List;
import java.util.Optional;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

public interface ItemRepository extends MongoRepository<ItemEntity, String> {
  
  List<ItemEntity> findByName(String name);

  @Query("{ 'attributes': { $elemMatch: { $in: ?0 } } }")
  List<ItemEntity> findItemsByAttributesAttributeIn(List<String> attributeList);
}

