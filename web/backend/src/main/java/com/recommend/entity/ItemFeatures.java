package com.recommend.entity;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@TableName("item_features")
public class ItemFeatures {
    @TableId
    private Long itemId;
    private Long categoryId;
    private Integer pvCount;
    private Integer buyCount;
    private BigDecimal popularityScore;
    private LocalDateTime updateTime;
}
