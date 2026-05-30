package com.recommend.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Set;

@Service
public class ScheduledTaskService {

    @Autowired
    private DataService dataService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 每 5 分钟执行一次：更新用户特征和商品特征，并刷新Redis缓存
     * cron表达式：秒 分 时 日 月 周
     * 0 每隔5分钟 * * * ? 表示每 5 分钟执行一次
     */
//    @Scheduled(cron = "0 */5 * * * ?")
//    @Scheduled(fixedRate = 10000)  // 每10秒执行一次
    @Scheduled(fixedDelay = 20000)  // 上次任务完成后等 20 秒再执行下一次
    public void updateFeaturesEvery5Minutes() {
        System.out.println("=== 开始执行特征更新任务 ===");
        try {
            dataService.updateUserFeatures();
            dataService.updateItemFeatures();
            
            System.out.println("=== 开始刷新 Redis 缓存 ===");
            refreshRedisCache();
            System.out.println("=== 特征更新任务完成 ===");
        } catch (Exception e) {
            System.err.println("特征更新任务失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 刷新Redis缓存（用户特征和商品特征）
     */
    private void refreshRedisCache() {
        try {
            Set<String> userFeatureKeys = stringRedisTemplate.keys("uf:*");
            if (userFeatureKeys != null && !userFeatureKeys.isEmpty()) {
                stringRedisTemplate.delete(userFeatureKeys);
                System.out.println("[Redis Cache] 已清除 " + userFeatureKeys.size() + " 个用户特征缓存 (uf:*)");
            }

            Set<String> itemFeatureKeys = stringRedisTemplate.keys("if:*");
            if (itemFeatureKeys != null && !itemFeatureKeys.isEmpty()) {
                stringRedisTemplate.delete(itemFeatureKeys);
                System.out.println("[Redis Cache] 已清除 " + itemFeatureKeys.size() + " 个商品特征缓存 (if:*)");
            }
        } catch (Exception e) {
            System.err.println("[Redis Cache] 刷新缓存失败: " + e.getMessage());
        }
    }

    /**
     * 每天凌晨2点执行：可选的深度清理任务
     */
    @Scheduled(cron = "0 0 2 * * ?")
    public void dailyCleanup() {
        System.out.println("=== 开始执行每日清理任务 ===");
        try {
            Set<String> expiredKeys = stringRedisTemplate.keys("rec:*");
            if (expiredKeys != null && !expiredKeys.isEmpty()) {
                int count = 0;
                for (String key : expiredKeys) {
                    Long ttl = stringRedisTemplate.getExpire(key);
                    if (ttl != null && ttl < 3600) {
                        stringRedisTemplate.delete(key);
                        count++;
                    }
                }
                System.out.println("[Daily Cleanup] 已清理 " + count + " 个即将过期的推荐缓存");
            }
        } catch (Exception e) {
            System.err.println("[Daily Cleanup] 清理任务失败: " + e.getMessage());
        }
        System.out.println("=== 每日清理任务完成 ===");
    }
}
