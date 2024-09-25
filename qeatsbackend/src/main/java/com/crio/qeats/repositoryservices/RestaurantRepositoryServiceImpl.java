/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.repositoryservices;

import ch.hsr.geohash.GeoHash;
import com.crio.qeats.configs.RedisConfiguration;
import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.globals.GlobalConstants;
import com.crio.qeats.models.ItemEntity;
import com.crio.qeats.models.MenuEntity;
import com.crio.qeats.models.RestaurantEntity;
import com.crio.qeats.repositories.ItemRepository;
import com.crio.qeats.repositories.MenuRepository;
import com.crio.qeats.repositories.RestaurantRepository;
import com.crio.qeats.utils.GeoLocation;
import com.crio.qeats.utils.GeoUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Provider;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;


@Service
public class RestaurantRepositoryServiceImpl implements RestaurantRepositoryService {

  @Autowired 
  private RestaurantRepository restaurantRepository;

  @Autowired
  private MenuRepository menuRepository;

  @Autowired
  private ItemRepository itemRepository;

  @Autowired
  private RedisConfiguration redisConfiguration;

  @Autowired
  private MongoTemplate mongoTemplate;

  @Autowired
  private Provider<ModelMapper> modelMapperProvider;

  private boolean isOpenNow(LocalTime time, RestaurantEntity res) {
    LocalTime openingTime = LocalTime.parse(res.getOpensAt());
    LocalTime closingTime = LocalTime.parse(res.getClosesAt());

    return time.isAfter(openingTime) && time.isBefore(closingTime);
  }

  // TODO: CRIO_TASK_MODULE_NOSQL
  // Objectives:
  // 1. Implement findAllRestaurantsCloseby.
  // 2. Remember to keep the precision of GeoHash in mind while using it as a key.
  // Check RestaurantRepositoryService.java file for the interface contract.
  public List<Restaurant> findAllRestaurantsCloseBy(Double latitude,
      Double longitude, LocalTime currentTime, Double servingRadiusInKms) {

    List<Restaurant> restaurants = null;
    if (redisConfiguration.isCacheAvailable()) {
      restaurants = findAllRestaurantsCloseByFromCache(
        latitude, longitude, currentTime, servingRadiusInKms);
    } else {
      restaurants = findAllRestaurantsCloseFromDb(
        latitude, longitude, currentTime, servingRadiusInKms);
    }
    return restaurants;
      

  }

  private List<Restaurant> findAllRestaurantsCloseFromDb(
      Double latitude, Double longitude, LocalTime currentTime,
      Double servingRadiusInKms) {
    List<Restaurant> restaurants = new ArrayList<Restaurant>();
    List<RestaurantEntity> restaurantEntities = restaurantRepository.findAll();

    for (RestaurantEntity restaurantEntity : restaurantEntities) {
      if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime,
          latitude, longitude, servingRadiusInKms)) {
        Restaurant restaurant = modelMapperProvider.get().map(
            restaurantEntity,Restaurant.class);
        restaurants.add(restaurant);
      }
    }

    return restaurants;
  }

  private List<Restaurant> findAllRestaurantsCloseByFromCache(
      Double latitude, Double longitude, LocalTime currentTime,
      Double servingRadiusInKms) {
    List<Restaurant> restaurantList = new ArrayList<>();

    GeoLocation geoLocation = new GeoLocation(latitude, longitude);
    GeoHash geoHash = GeoHash.withCharacterPrecision(geoLocation.getLatitude(), 
        geoLocation.getLongitude(), 7);

    try (Jedis jedis = redisConfiguration.getJedisPool().getResource()) {
      String jsonStringFromCache = jedis.get(geoHash.toBase32());

      if (jsonStringFromCache == null) {
        // Cache needs to be updated.
        String createdJsonString = "";
        try {
          restaurantList = findAllRestaurantsCloseFromDb(
            geoLocation.getLatitude(), geoLocation.getLongitude(),
              currentTime, servingRadiusInKms);
          createdJsonString = new ObjectMapper().writeValueAsString(restaurantList);
        } catch (JsonProcessingException e) {
          e.printStackTrace();
        }

        // Do operations with jedis resource
        jedis.setex(geoHash.toBase32(), GlobalConstants.REDIS_ENTRY_EXPIRY_IN_SECONDS, 
            createdJsonString);
      } else {
        try {
          restaurantList = new ObjectMapper().readValue(jsonStringFromCache, 
          new TypeReference<List<Restaurant>>() {
            });
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    return restaurantList;
  }






  // TODO: CRIO_TASK_MODULE_NOSQL
  // Objective:
  // 1. Check if a restaurant is nearby and open. If so, it is a candidate to be returned.
  // NOTE: How far exactly is "nearby"?

  /**
   * Utility method to check if a restaurant is within the serving radius at a given time.
   * @return boolean True if restaurant falls within serving radius and is open, false otherwise
   */
  private boolean isRestaurantCloseByAndOpen(RestaurantEntity restaurantEntity,
      LocalTime currentTime, Double latitude, Double longitude, Double servingRadiusInKms) {
    if (isOpenNow(currentTime, restaurantEntity)) {
      return GeoUtils.findDistanceInKm(latitude, longitude,restaurantEntity.getLatitude(), 
      restaurantEntity.getLongitude()) < servingRadiusInKms;
    }

    return false;
  }

  @Override
  public List<Restaurant> findRestaurantsByName(Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    List<Restaurant> response = findAllRestaurantsCloseBy(latitude,
        longitude,  currentTime,  servingRadiusInKms);

    LinkedHashSet<Restaurant> response1 = new LinkedHashSet<>();

    response1.addAll(response.stream()
        .filter(a -> a.getName().equals(searchString))
        .collect(Collectors.toList()));
    response1.addAll(response.stream()
        .filter(a -> a.getName().contains(searchString))
        .collect(Collectors.toList()));
    
    return response1.stream().collect(Collectors.toList());
  }

  @Override
  public List<Restaurant> findRestaurantsByAttributes(Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    List<Restaurant> result = findAllRestaurantsCloseBy(latitude,
        longitude,  currentTime,  servingRadiusInKms);
    List<Restaurant> response = new ArrayList<>();

    for(Restaurant restaurant : result){
     for(String attribute : restaurant.getAttributes()){
      if(attribute.equals(searchString)){
        response.add(restaurant);
        break;
      }
     }
    }
    return response;
  }

  @Override
  public List<Restaurant> findRestaurantsByItemName(Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {

    List<Restaurant> restaurants = new ArrayList<>();
    
    List<ItemEntity> items = itemRepository.findByName(searchString);
    List<String> itemIdList = new ArrayList<>();
    for (ItemEntity item : items) {
      itemIdList.add(item.getItemId());
    }
    List<MenuEntity> menus = menuRepository.findMenusByItemsItemIdIn(itemIdList).get();
    List<String> restaurantIds = new ArrayList<>();
    for (MenuEntity menu : menus) {
      restaurantIds.add(menu.getRestaurantId());
    }
    List<RestaurantEntity> restaurantEntities = restaurantRepository.findRestaurantsById(restaurantIds).get();

    for (RestaurantEntity restaurantEntity : restaurantEntities) {
      if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime,
          latitude, longitude, servingRadiusInKms)) {
        Restaurant restaurant = modelMapperProvider.get().map(
            restaurantEntity,Restaurant.class);
        restaurants.add(restaurant);
      }
    }

    return restaurants;
  }

  @Override
  public List<Restaurant> findRestaurantsByItemAttributes(Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {

    List<Restaurant> restaurants = new ArrayList<>();
    
    List<String> attributesList = new ArrayList<>();
    attributesList.add(searchString);
    List<ItemEntity> items = itemRepository.findItemsByAttributesAttributeIn(attributesList);

    List<String> itemIdList = new ArrayList<>();
    for (ItemEntity item : items) {
      itemIdList.add(item.getItemId());
    }

    List<MenuEntity> menus = menuRepository.findMenusByItemsItemIdIn(itemIdList).get();
    List<String> restaurantIds = new ArrayList<>();
    for (MenuEntity menu : menus) {
      restaurantIds.add(menu.getRestaurantId());
    }

    List<RestaurantEntity> restaurantEntities = restaurantRepository.findRestaurantsById(restaurantIds).get();

    for (RestaurantEntity restaurantEntity : restaurantEntities) {
      if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime,
          latitude, longitude, servingRadiusInKms)) {
        Restaurant restaurant = modelMapperProvider.get().map(
            restaurantEntity,Restaurant.class);
        restaurants.add(restaurant);
      }
    }

    return restaurants;
  }

}

