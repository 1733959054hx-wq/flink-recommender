package com.recommend.entity;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@TableName("user_features")
public class UserFeatures {
    @TableId
    private Long userId;
    private Integer pvCount;
    private Integer buyCount;
    private Integer cartCount;
    private Integer favCount;
    private BigDecimal buyRate;
    private BigDecimal activeScore;
    private Long highFreqCategory;
    private LocalDateTime updateTime;
}
