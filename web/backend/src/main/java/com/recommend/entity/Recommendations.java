package com.recommend.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@TableName("recommendations")
public class Recommendations {
    @TableId(type = IdType.AUTO)
    private Long id;
    private Long userId;
    private Long itemId;
    private BigDecimal predictScore;
    private BigDecimal popularityScore;
    private BigDecimal finalScore;
    private Integer rankNo;
    private String recommendType;
    private LocalDateTime createTime;
}
