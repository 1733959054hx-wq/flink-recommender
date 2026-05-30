import axios from 'axios'
import { ElMessage } from 'element-plus'

const apiClient = axios.create({
  baseURL: '/api',
  timeout: 300000,
  headers: {
    'Content-Type': 'application/json'
  }
})

apiClient.interceptors.request.use(
    config => {
      return config
    },
    error => {
      console.error('Request error:', error)
      return Promise.reject(error)
    }
)

apiClient.interceptors.response.use(
    response => {
        const data = response.data;
        if (!data) {
            return response;
        }
        // ★ 修复：对于插入行为接口的特殊处理
        // "success" 也算成功，不弹窗
        if (data.code !== 200 && data.code !== undefined) {
            // ★ 只对非插入类接口的报错弹窗
            // 插入类接口的报错已经被后端吞噬，不会走到这里
            ElMessage.error(data.message || '请求失败')
            return Promise.reject(new Error(data.message || '请求失败'))
        }
        return data
    },
    error => {
        // ★ 修复：区分网络错误和处理
        if (error.response) {
            const status = error.response.status;
            if (status >= 400 && status < 500) {
                ElMessage.error(error.response.data?.message || `请求错误(${status})`)
            } else if (status >= 500) {
                console.error('服务端错误:', status, error.response.data)
                // 500错误只打印日志，不弹窗（可能是临时异常）
            }
        } else if (error.code === 'ECONNABORTED') {
            console.warn('请求超时:', error.message)
            // 超时不弹窗
        } else {
            console.warn('网络错误:', error.message)
            // 其他网络错误不弹窗
        }
        return Promise.reject(error)
    }
)


export const api = {
  // 用户行为
  getBehaviorDistribution: () => apiClient.get('/behavior/distribution'),
  getBehaviorFunnel: () => apiClient.get('/behavior/funnel'),
  getUserRecentBehaviors: (userId, limit = 10) => apiClient.get('/behavior/recent', { params: { userId, limit } }),
  insertBehavior: (data) => apiClient.post('/behavior/insert', data),

  // 推荐查询
  getLatestRecommendations: (userId) => apiClient.get('/recommend/latest', { params: { userId } }),
  getPredictionStats: () => apiClient.get('/recommend/prediction-stats'),
  getTop10Items: () => apiClient.get('/item/top10'),
  getItemList: (keyword) => apiClient.get('/item/list', { params: { keyword } }),
  getCategoryList: () => apiClient.get('/category/list'),

  // ===== PR-AUC 相关 =====
  getPrAuc: () => apiClient.get('/metrics/pr-auc'),                                      // 最新PR-AUC
  getPrAucTrend: (n = 6) => apiClient.get('/metrics/pr-auc-trend', { params: { n } }),  // PR-AUC趋势

  // ===== CTR/CVR 需要时间段参数 =====
  getCtrCvr: (startTime, endTime) => apiClient.get('/recommend/ctr-cvr', { params: { startTime, endTime } }),
  getCtrCvrTrend: (startTime, endTime) => apiClient.get('/recommend/ctr-cvr-trend', { params: { startTime, endTime } }),

  // ===== 推荐次数趋势需要时间段参数 =====
  getRecommendCountTrend: (startTime, endTime) => apiClient.get('/recommend/count-trend', { params: { startTime, endTime } }),

  // 用户相关
  getUserRadar: (userId) => apiClient.get('/user/radar', { params: { userId } }),
  getUserList: () => apiClient.get('/user/list'),

  // 模型管理
  retrainModel: () => apiClient.post('/model/retrain'),
  evaluateModel: () => apiClient.post('/model/evaluate'),   // 【新增】模型评测

  // 通用
  health: () => apiClient.get('/health')
}

export default apiClient
