package com.example.seckill.controller;

import com.example.seckill.service.OrderTxService;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/pay")
public class PaymentController {

    private final OrderTxService orderTxService;

    public PaymentController(OrderTxService orderTxService) {
        this.orderTxService = orderTxService;
    }

    @PostMapping("/{orderNo}")
    public Map<String, Object> pay(@PathVariable String orderNo) {
        boolean success = orderTxService.pay(orderNo);
        return Map.of(
                "success", success,
                "message", success
                        ? "支付成功"
                        : "订单不是 WAIT_PAY，可能已支付或已关闭"
        );
    }
}
