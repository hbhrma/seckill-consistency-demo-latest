package com.example.seckill.controller;

import com.example.seckill.dto.SeckillResponse;
import com.example.seckill.service.SeckillEntryService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/seckill")
public class SeckillController {

    private final SeckillEntryService seckillEntryService;

    public SeckillController(SeckillEntryService seckillEntryService) {
        this.seckillEntryService = seckillEntryService;
    }

    @PostMapping("/{goodsId}")
    public SeckillResponse seckill(@PathVariable long goodsId,
                                   @RequestParam long userId) {
        return seckillEntryService.seckill(userId, goodsId);
    }
}
