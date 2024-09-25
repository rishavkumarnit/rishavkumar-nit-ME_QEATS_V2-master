
/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.services;

import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.exchanges.GetRestaurantsRequest;
import com.crio.qeats.exchanges.GetRestaurantsResponse;
import com.crio.qeats.repositoryservices.RestaurantRepositoryService;
import java.time.LocalTime;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Log4j2
public class RestaurantServiceImpl implements RestaurantService {

  private final Double peakHoursServingRadiusInKms = 3.0;
  private final Double normalHoursServingRadiusInKms = 5.0;
  
  @Autowired
  private RestaurantRepositoryService restaurantRepositoryService;


  // TODO: CRIO_TASK_MODULE_RESTAURANTSAPI - Implement findAllRestaurantsCloseby.
  // Check RestaurantService.java file for the interface contract.
  @Override
  public GetRestaurantsResponse findAllRestaurantsCloseBy(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {

    boolean isPeakHourRequest = checkifPeakHourRequest(currentTime);
    List<Restaurant> restaurants;

    if (isPeakHourRequest) {
      restaurants = restaurantRepositoryService.findAllRestaurantsCloseBy(
      getRestaurantsRequest.getLatitude(),  getRestaurantsRequest.getLongitude(),
        currentTime,  peakHoursServingRadiusInKms);
    } else {
      restaurants = restaurantRepositoryService.findAllRestaurantsCloseBy(
      getRestaurantsRequest.getLatitude(),  getRestaurantsRequest.getLongitude(),
      currentTime, normalHoursServingRadiusInKms);
    }

    for (Restaurant restaurant : restaurants) {
      String rectifiedName = restaurant.getName().replaceAll("[Â©éí]", "e");
      restaurant.setName(rectifiedName);
    }

    return new GetRestaurantsResponse(restaurants);
  }



  private boolean checkifPeakHourRequest(LocalTime currentTime) {
    if (currentTime.isAfter(LocalTime.of(7, 59)) && currentTime
        .isBefore(LocalTime.of(10, 0).plusMinutes(1))) {
      return true;
    }
    if (currentTime.isAfter(LocalTime.of(12, 59)) && currentTime
        .isBefore(LocalTime.of(14, 0).plusMinutes(1))) {
      return true;
    }
    if (currentTime.isAfter(LocalTime.of(18, 59)) && currentTime
        .isBefore(LocalTime.of(21, 0).plusMinutes(1))) {
      return true;
    }
    return false;

  }


  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Implement findRestaurantsBySearchQuery. The request object has the search string.
  // We have to combine results from multiple sources:
  // 1. Restaurants by name (exact and inexact)
  // 2. Restaurants by cuisines (also called attributes)
  // 3. Restaurants by food items it serves
  // 4. Restaurants by food item attributes (spicy, sweet, etc)
  // Remember, a restaurant must be present only once in the resulting list.
  // Check RestaurantService.java file for the interface contract.
  @Override
  public GetRestaurantsResponse findRestaurantsBySearchQuery(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {
    
    if(getRestaurantsRequest.getSearchFor().equals("")){
      return new GetRestaurantsResponse(new ArrayList<>());
    }
    
    LinkedHashSet<Restaurant> responseList = new LinkedHashSet<>();
    Double radius;

    boolean isPeakHourRequest = checkifPeakHourRequest(currentTime);
    if (isPeakHourRequest) {
      radius = peakHoursServingRadiusInKms;
    } else {
      radius = normalHoursServingRadiusInKms;
    }

    responseList.addAll(restaurantRepositoryService.findRestaurantsByName(
        getRestaurantsRequest.getLatitude(), getRestaurantsRequest.getLongitude(),
        getRestaurantsRequest.getSearchFor(), currentTime, radius));

    responseList.addAll(restaurantRepositoryService.findRestaurantsByAttributes(
        getRestaurantsRequest.getLatitude(), getRestaurantsRequest.getLongitude(),
        getRestaurantsRequest.getSearchFor(), currentTime, radius));

    responseList.addAll(restaurantRepositoryService.findRestaurantsByItemName(
        getRestaurantsRequest.getLatitude(), getRestaurantsRequest.getLongitude(),
        getRestaurantsRequest.getSearchFor(), currentTime, radius));

    responseList.addAll(restaurantRepositoryService.findRestaurantsByItemAttributes(
        getRestaurantsRequest.getLatitude(), getRestaurantsRequest.getLongitude(),
        getRestaurantsRequest.getSearchFor(), currentTime, radius));
    
    

    return new GetRestaurantsResponse(responseList.stream().collect(Collectors.toList()));
  }

  // TODO: CRIO_TASK_MODULE_MULTITHREADING
  // Implement multi-threaded version of RestaurantSearch.
  // Implement variant of findRestaurantsBySearchQuery which is at least 1.5x time faster than
  // findRestaurantsBySearchQuery.
  @Override
  public GetRestaurantsResponse findRestaurantsBySearchQueryMt(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {

    List<Restaurant> restaurantList = new ArrayList<>();
    String searchQuery = getRestaurantsRequest.getSearchFor();
    if(searchQuery==""){
      return new GetRestaurantsResponse(restaurantList);
    }

    Double latitude = getRestaurantsRequest.getLatitude();
    Double longitude = getRestaurantsRequest.getLongitude();
    Double radius;

    boolean isPeakHourRequest = checkifPeakHourRequest(currentTime);
    if (isPeakHourRequest) {
      radius = peakHoursServingRadiusInKms;
    } else {
      radius = normalHoursServingRadiusInKms;
    }

    ExecutorService threadPool = Executors.newFixedThreadPool(4);
    
    threadPool.execute(() -> {
      synchronized (restaurantList) {
        restaurantList.addAll(restaurantRepositoryService.findRestaurantsByName(latitude, longitude, searchQuery, currentTime, radius));
      }
    });

    threadPool.execute(() -> {
      synchronized (restaurantList) {
        restaurantList.addAll(restaurantRepositoryService.findRestaurantsByAttributes(latitude, longitude, searchQuery, currentTime, radius));
      }
    });

    threadPool.execute(() -> {
      synchronized (restaurantList) {
        restaurantList.addAll(restaurantRepositoryService.findRestaurantsByItemName(latitude, longitude, searchQuery, currentTime, radius));
      }
    });

    threadPool.execute(() -> {
      synchronized (restaurantList) {
        restaurantList.addAll(restaurantRepositoryService.findRestaurantsByItemAttributes(latitude, longitude, searchQuery, currentTime, radius));
      }
    });

    return new GetRestaurantsResponse(restaurantList);
  }
}

