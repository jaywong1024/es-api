package com.es.api.controller;

import com.es.api.service.SearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
public class SearchController {

    @Autowired
    private SearchService searchService;

    @PostMapping("/jd")
    public boolean addDocumentFromJd(@RequestBody Map<String, Object> params) {
        try {
            return this.searchService.addDocumentFromJd((String) params.get("keyword"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @GetMapping("/jd/{keyword}/{page}/{size}")
    public List<Object> search(@PathVariable("keyword") String keyword,
                                  @PathVariable("page") Integer page, @PathVariable("size") Integer size) {
        try {
            return searchService.search(keyword, page, size);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

}
