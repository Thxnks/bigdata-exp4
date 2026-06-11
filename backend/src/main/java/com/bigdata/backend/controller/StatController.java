package com.bigdata.backend.controller;

import com.bigdata.backend.entity.StatResult;
import com.bigdata.backend.repository.StatResultRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/stat")
public class StatController {

    private final StatResultRepository repository;

    public StatController(StatResultRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/list")
    public List<StatResult> list() {
        return repository.findAll();
    }
}
