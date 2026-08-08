package com.antontech.itemkafka_poc.controller;

import com.antontech.itemkafka_poc.model.Item;
import com.antontech.itemkafka_poc.repos.ItemRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-only REST entry point that powers the React front end's paginated
 * Item grid view. Backed directly by {@link ItemRepository} (JPA), so it
 * works against whichever database {@code spring.datasource} currently
 * points at (MS SQL Server or MySQL).
 */
@Slf4j
@RestController
@RequestMapping(path = "item-kafka/app/")
@Tag(name = "Item Kafka Producer and Consumer Controller", description = "The Item Kafka Producer and Consumer API")
public class ItemController {

    @Autowired
    private ItemRepository itemRepository;

    /**
     * Returns a single page of {@link Item} rows, sorted by {@code itemId},
     * for display in the front end's grid/pager component.
     *
     * @param page zero-based page index (default {@code 0}).
     * @param size number of records per page (default {@code 15}).
     * @return HTTP 200 with a Spring Data {@link Page} of items (content, totalElements, totalPages, etc.).
     */
    @Operation(summary = "List items (paginated)", description = "Returns a single page of Item rows for the grid view.", tags = {"msg"})
    @GetMapping(path = "items/v1", produces = "application/json")
    public Page<Item> listItems(@RequestParam(defaultValue = "0") int page,
                                 @RequestParam(defaultValue = "15") int size) {
        Page<Item> result = itemRepository.findAll(PageRequest.of(page, size, Sort.by("itemId").ascending()));
        log.debug("Returning items page {} of size {} (totalElements={})", page, size, result.getTotalElements());
        return result;
    }

    /**
     * @return HTTP 200 with the total number of Item rows currently in the source table.
     */
    @Operation(summary = "Count items", description = "Returns the total number of Item rows in the source table.", tags = {"msg"})
    @GetMapping(path = "items/count/v1", produces = "application/json")
    public long countItems() {
        return itemRepository.count();
    }
}

