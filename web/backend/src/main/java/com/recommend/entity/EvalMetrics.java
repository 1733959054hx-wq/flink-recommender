package com.recommend.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@TableName("eval_metrics")
public class EvalMetrics {
    @TableId(type = IdType.AUTO)
    private Long id;
    private LocalDateTime metricStartTime;
    private LocalDateTime metricEndTime;
    @TableField("pr_auc")
    private BigDecimal accuracy;
    private BigDecimal ctr;
    private BigDecimal cvr;
    private Integer recommendCount;
    private LocalDateTime createTime;
}