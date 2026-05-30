package com.recommend.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.recommend.entity.Recommendations;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface RecommendationsMapper extends BaseMapper<Recommendations> {
}
