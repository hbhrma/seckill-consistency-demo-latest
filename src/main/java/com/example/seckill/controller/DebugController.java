package com.example.seckill.controller;

import com.example.seckill.redis.RedisKeys;
import com.example.seckill.redis.RedisReservationService;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 仅用于本地演示，请勿原样放到生产环境。
 */
@RestController
@RequestMapping("/debug")
public class DebugController {

    private final RedisReservationService reservationService;
    private final StringRedisTemplate redisTemplate;

    public DebugController(RedisReservationService reservationService,
                           StringRedisTemplate redisTemplate) {
        this.reservationService = reservationService;
        this.redisTemplate = redisTemplate;
    }

    @PostMapping("/stock/{goodsId}/{stock}")
    public Map<String, Object> init(@PathVariable long goodsId,
                                    @PathVariable long stock) {
        reservationService.initializeStock(goodsId, stock);
        return Map.of("goodsId", goodsId, "stock", stock);
    }

    @GetMapping("/stock/{goodsId}")
    public Map<String, Object> stock(@PathVariable long goodsId) {
        String value = redisTemplate.opsForValue().get(RedisKeys.stock(goodsId));
        Long buyers = redisTemplate.opsForSet().size(RedisKeys.buyers(goodsId));
        return Map.of(
                "goodsId", goodsId,
                "stock", value == null ? "null" : value,
                "buyerCount", buyers == null ? 0 : buyers
        );
    }
}
