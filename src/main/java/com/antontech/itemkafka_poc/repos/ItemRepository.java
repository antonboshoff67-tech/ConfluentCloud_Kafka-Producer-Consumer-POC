package com.antontech.itemkafka_poc.repos;

//import org.springframework.data.jpa.repository.JpaRepository;
//import org.springframework.stereotype.Repository;
//import com.antontech.itemkafka_poc.model.Item;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import com.antontech.itemkafka_poc.model.Item;

import java.util.List;

@Repository
public interface ItemRepository extends JpaRepository<Item, String> {
    List<Item> findFirst100ByItemIdIsNotNull();
}

