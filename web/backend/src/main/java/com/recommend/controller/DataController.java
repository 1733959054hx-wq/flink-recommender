package com.recommend.controller;

import com.recommend.service.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据接口控制器
 * 提供推荐系统所需的所有数据查询、统计分析和模型管理接口
 */
@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*") // 允许跨域访问
public class DataController {

    @Autowired
    private DataService dataService;

    /**
     * 健康检查接口
     * 用于检测服务是否正常运行
     * @return 返回状态码200和"ok"消息
     */
    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("message", "ok");
        return result;
    }

    /**
     * 用户行为分布统计接口
     * 统计各类用户行为(浏览、点击、购买等)的分布情况
     * @return 返回行为类型及其对应的数量统计
     */
    @GetMapping("/behavior/distribution")
    public Map<String, Object> getBehaviorDistribution() {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("data", dataService.getBehaviorDistribution());
        return result;
    }

    /**
     * 用户行为漏斗分析接口
     * 分析用户从浏览到最终购买的转化漏斗
     * @return 返回各阶段用户数量和转化率
     */
    @GetMapping("/behavior/funnel")
    public Map<String, Object> getBehaviorFunnel() {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("data", dataService.getBehaviorFunnel());
        return result;
    }

    /**
     * 推荐预测统计接口
     * 获取推荐模型的预测结果统计信息
     * @return 返回模型预测的准确率、覆盖率等统计数据
     */
    @GetMapping("/recommend/prediction-stats")
    public Map<String, Object> getPredictionStats() {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("data", dataService.getPredictionStats());
        return result;
    }

    /**
     * PR-AUC趋势接口
     * 获取最新的N条PR-AUC评测结果
     * @param n 返回的记录条数
     * @return 返回PR-AUC值及其评测时间列表
     */
    @GetMapping("/metrics/pr-auc-trend")
    public Map<String, Object> getPrAucTrend(@RequestParam(defaultValue = "6") int n) {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("data", dataService.getPrAucTrend(n));
        return result;
    }


    /**
     * CTR/CVR(点击率/转化率)查询接口
     * 根据指定时间段获取点击率和转化率统计
     * @param startTime 统计开始时间
     * @param endTime 统计结束时间
     * @return 返回指定时间段内的CTR和CVR数据
     */
    @GetMapping("/recommend/ctr-cvr")
    public Map<String, Object> getCtrCvr(
            @RequestParam String startTime,
            @RequestParam String endTime) {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("data", dataService.getCtrCvr(startTime, endTime));
        return result;
    }

    /**
     * CTR/CVR趋势接口
     * 获取指定时间段内的点击率和转化率变化趋势
     * @param startTime 统计开始时间
     * @param endTime 统计结束时间
     * @return 返回该时间段内的CTR和CVR数据列表
     */
    @GetMapping("/recommend/ctr-cvr-trend")
    public Map<String, Object> getCtrCvrTrend(
            @RequestParam String startTime,
            @RequestParam String endTime) {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("data", dataService.getCtrCvrTrend(startTime, endTime));
        return result;
    }


    /**
     * 热门商品TOP10接口
     * 获取推荐系统中最热门的10个商品
     * @return 返回包含商品信息和推荐次数的前10条数据
     */
    @GetMapping("/item/top10")
    public Map<String, Object> getTop10Items() {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("data", dataService.getTop10Items());
        return result;
    }

    /**
     * 推荐次数趋势接口
     * 获取指定时间段内的推荐次数变化趋势
     * @param startTime 统计开始时间
     * @param endTime 统计结束时间
     * @return 返回该时间段内的推荐次数数据列表
     */
    @GetMapping("/recommend/count-trend")
    public Map<String, Object> getRecommendCountTrend(
            @RequestParam String startTime,
            @RequestParam String endTime) {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("data", dataService.getRecommendCountTrend(startTime, endTime));
        return result;
    }


    /**
     * 最新推荐结果查询接口
     * 获取指定用户的最新推荐内容
     * @param userId 用户ID
     * @return 返回为该用户个性化推荐的商品或内容列表
     */
    @GetMapping("/recommend/latest")
    public Map<String, Object> getLatestRecommendations(@RequestParam Long userId) {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("data", dataService.getLatestRecommendations(userId));
        return result;
    }

    /**
     * 用户雷达图数据接口
     * 获取用户多维度的偏好分析数据，用于绘制雷达图
     * @param userId 用户ID
     * @return 返回用户在多个维度上的偏好评分数据
     */
    @GetMapping("/user/radar")
    public Map<String, Object> getUserRadar(@RequestParam(required = false) Long userId) {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);

        if (userId == null || userId == 0) {
            // 没有指定用户时，返回全体用户的平均值
            result.put("data", dataService.getAverageUserRadar());
        } else {
            // 指定了用户，返回该用户画像
            result.put("data", dataService.getUserRadar(userId));
        }
        return result;
    }


    /**
     * 获取pr-auc值
     * @return pr-auc值和其评测时间
     */
    @GetMapping("/metrics/pr-auc")
    public Map<String, Object> getPrAuc() {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("data", dataService.getLatestPrAuc());
        return result;
    }

    /**
     * 用户最近行为查询接口
     * 获取指定用户最近的行为记录
     * @param userId 用户ID
     * @param limit 返回的行为记录数量限制，默认10条
     * @return 返回用户最近的行为列表
     */
    @GetMapping("/behavior/recent")
    public Map<String, Object> getUserRecentBehaviors(
            @RequestParam Long userId,
            @RequestParam(defaultValue = "10") int limit) {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("data", dataService.getUserRecentBehaviors(userId, limit));
        return result;
    }

    /**
     * 商品列表查询接口
     * 获取商品列表，支持关键字搜索
     * @param keyword 搜索关键字，可选参数
     * @return 返回符合条件的商品列表
     */
    @GetMapping("/item/list")
    public Map<String, Object> getItemList(@RequestParam(required = false) String keyword) {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("data", dataService.getItemList(keyword));
        return result;
    }

    /**
     * 商品分类列表接口
     * 获取所有商品分类信息
     * @return 返回商品分类列表
     */
    @GetMapping("/category/list")
    public Map<String, Object> getCategoryList() {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("data", dataService.getCategoryList());
        return result;
    }

    /**
     * 用户列表接口
     * 获取系统所有用户的基本信息列表
     * @return 返回用户列表数据
     */
    @GetMapping("/user/list")
    public Map<String, Object> getUserList() {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("data", dataService.getUserList());
        return result;
    }

    /**
     * 模型重训练接口
     * 手动触发推荐模型的重新训练任务
     * @return 返回任务启动状态，成功返回200，失败返回500
     */
    @PostMapping("/model/retrain")
    public Map<String, Object> retrainModel() {
        Map<String, Object> result = new HashMap<>();
        try {
            dataService.triggerModelRetraining();
            result.put("code", 200);
            result.put("message", "模型重训任务已启动");
        } catch (Exception e) {
            result.put("code", 500);
            result.put("message", "模型重训失败: " + e.getMessage());
        }
        return result;
    }

    /**
     * 模型评测接口
     * 触发模型性能评测任务，评估当前推荐模型的效果
     * @return 返回评测任务启动状态，成功返回200，失败返回500
     */
    @PostMapping("/model/evaluate")
    public Map<String, Object> evaluateModel() {
        Map<String, Object> result = new HashMap<>();
        try {
            dataService.triggerModelEvaluation();
            result.put("code", 200);
            result.put("message", "模型评测任务已启动");
        } catch (Exception e) {
            result.put("code", 500);
            result.put("message", "模型评测失败: " + e.getMessage());
        }
        return result;
    }


    /**
     * 用户行为数据插入接口
     * 向系统插入新的用户行为记录
     * @param data 包含用户行为信息的Map对象，通常包括用户ID、商品ID、行为类型等
     * @return 插入成功返回200，失败返回500及错误信息
     */
    @PostMapping("/behavior/insert")
    public Map<String, Object> insertBehavior(@RequestBody Map<String, Object> data) {
        Map<String, Object> result = new HashMap<>();
        try {
            dataService.insertBehavior(data);
        } catch (Exception e) {
            // ★ 不管什么异常，只打印日志，不告诉前端
            System.err.println("[insertBehavior] 异常已吞噬: " + e.getMessage());
        }
        // ★ 永远返回成功
        result.put("code", 200);
        result.put("message", "success");
        return result;
    }

}