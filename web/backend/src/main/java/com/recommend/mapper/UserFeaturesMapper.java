package com.recommend.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.recommend.entity.UserFeatures;
import org.apache.ibatis.annotations.*;
import java.util.List;
import java.util.Map;

@Mapper
public interface UserFeaturesMapper extends BaseMapper<UserFeatures> {

    @Insert({"<script>",
            "REPLACE INTO user_features ",
            "(user_id, pv_count, buy_count, cart_count, fav_count, buy_rate, ",
            "active_score, high_freq_category, update_time) VALUES ",
            "<foreach collection='list' item='item' separator=','>",
            "(#{item.userId}, #{item.pvCount}, #{item.buyCount}, #{item.cartCount}, ",
            "#{item.favCount}, #{item.buyRate}, #{item.activeScore}, ",
            "#{item.highFreqCategory}, NOW())",
            "</foreach>",
            "</script>"})
    int batchReplaceInto(@Param("list") List<UserFeatures> users);

    /**
     * 查询所有用户的平均画像指标
     */
    @Select({"SELECT " +
            "    AVG(pv_count) AS avg_pv, " +
            "    AVG(buy_count) AS avg_buy, " +
            "    AVG(cart_count) AS avg_cart, " +
            "    AVG(fav_count) AS avg_fav, " +
            "    AVG(buy_rate) AS avg_buy_rate, " +
            "    AVG(active_score) AS avg_active_score " +
            "FROM user_features"})
    Map<String, Object> selectAverageUserRadar();
}
