package com.recommend.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.recommend.entity.ItemFeatures;
import org.apache.ibatis.annotations.*;
import java.util.List;

@Mapper
public interface ItemFeaturesMapper extends BaseMapper<ItemFeatures> {

    /**
     * 批量替换商品特征（一次 SQL 写入所有商品）
     */
    @Insert({"<script>",
            "REPLACE INTO item_features ",
            "(item_id, category_id, pv_count, buy_count, popularity_score, update_time) VALUES ",
            "<foreach collection='list' item='item' separator=','>",
            "(#{item.itemId}, #{item.categoryId}, #{item.pvCount}, #{item.buyCount}, ",
            "#{item.popularityScore}, NOW())",
            "</foreach>",
            "</script>"})
    int batchReplaceInto(@Param("list") List<ItemFeatures> items);
}
