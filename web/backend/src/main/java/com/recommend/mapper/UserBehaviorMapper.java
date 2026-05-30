package com.recommend.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.recommend.entity.UserBehavior;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import java.util.List;
import java.util.Map;

@Mapper
public interface UserBehaviorMapper extends BaseMapper<UserBehavior> {

    /**
     * 一次查询：统计每个商品的浏览量、购买量、类目ID
     */
    @Select({"SELECT " +
            "    item_id, " +
            "    MAX(category_id) AS category_id, " +
            "    SUM(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) AS pv_count, " +
            "    SUM(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) AS buy_count " +
            "FROM user_behavior " +
            "GROUP BY item_id"})
    List<Map<String, Object>> selectItemFeatureAggregation();

    /**
     * 一次查询：统计每个用户的四种行为数量
     */
    @Select({"SELECT " +
            "    user_id, " +
            "    SUM(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) AS pv_count, " +
            "    SUM(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) AS buy_count, " +
            "    SUM(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END) AS cart_count, " +
            "    SUM(CASE WHEN behavior_type = 'fav' THEN 1 ELSE 0 END) AS fav_count " +
            "FROM user_behavior " +
            "GROUP BY user_id"})
    List<Map<String, Object>> selectUserFeatureAggregation();

    /**
     * 一次查询：统计每个用户的高频访问类目（交互最多的类目）
     */
    @Select({"SELECT user_id, category_id FROM (" +
            "    SELECT " +
            "        user_id, " +
            "        category_id, " +
            "        COUNT(*) AS cnt, " +
            "        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) AS rn " +
            "    FROM user_behavior " +
            "    GROUP BY user_id, category_id" +
            ") t WHERE rn = 1"})
    List<Map<String, Object>> selectUserHighFreqCategory();


}
