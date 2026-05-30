<template>
  <div class="dashboard" role="application" aria-label="实时电商数据大屏">
    <canvas ref="particleCanvas" class="particle-bg"></canvas>
    
    <header class="top-banner" role="banner">

      <h1 aria-live="polite">实时电商用户购买意向预测与推荐系统 - 监控大屏</h1>
      <!-- ===== 新增：时间范围选择器 ===== -->
      <div class="time-range-picker">
        <span class="time-label">查询时间段</span>
        <el-date-picker
            v-model="timeRange"
            type="datetimerange"
            range-separator="至"
            start-placeholder="开始时间"
            end-placeholder="结束时间"
            value-format="YYYY-MM-DD HH:mm:ss"
            :default-time="[new Date(2000, 0, 1, 0, 0, 0), new Date(2000, 0, 1, 23, 59, 59)]"
            style="width: 380px"
            :shortcuts="timeShortcuts"
            :disabled-date="disabledDate"
            @change="onTimeRangeChange"
        />
        <el-button type="primary" size="default" @click="onTimeRangeChange" :loading="chartsLoading">
          <el-icon><Search /></el-icon>
          查询
        </el-button>
        <span class="time-hint" v-if="timeLabel">{{ timeLabel }}</span>
      </div>
      <div class="banner-content">
        <div class="metric-card">
          <LiquidChart :option="liquidOption('accuracy')" :loading="chartsLoading" />
          <div class="metric-label">PR-AUC</div>
          <div class="metric-value">{{ (prAucData?.prAuc || 0).toFixed(4) }}</div>
          <div class="metric-sub-label" v-if="prAucData?.createTime">评测时间: {{ prAucData.createTime }}</div>
        </div>
        <div class="metric-card">
          <LiquidChart :option="liquidOption('ctr')" :loading="chartsLoading" />
          <div class="metric-label">CTR</div>
          <div class="metric-value">{{ formatPercent(ctrCvrData?.ctr || 0) }}</div>
          <div class="metric-sub-label" v-if="ctrCvrData?.startTime">{{ formatTime(ctrCvrData.startTime) }} ~ {{ formatTime(ctrCvrData.endTime) }}</div>
        </div>
        <div class="metric-card">
          <LiquidChart :option="liquidOption('cvr')" :loading="chartsLoading" />
          <div class="metric-label">CVR</div>
          <div class="metric-value">{{ formatPercent(ctrCvrData?.cvr || 0) }}</div>
          <div class="metric-sub-label" v-if="ctrCvrData?.startTime">{{ formatTime(ctrCvrData.startTime) }} ~ {{ formatTime(ctrCvrData.endTime) }}</div>
        </div>
        <div class="flip-card">
          <div class="flip-label">推荐总数</div>
          <div class="flip-value" :class="{ flipping: isFlipping }">
            <span class="flip-number">{{ displayRecommendCount }}</span>
          </div>
          <div class="metric-sub-label" v-if="timeRange?.length === 2">{{ formatTime(timeRange[0]) }} ~ {{ formatTime(timeRange[1]) }}</div>
        </div>
      </div>
    </header>

    <main class="main-layout" role="main">
      <aside class="left-sidebar" role="region" aria-label="用户行为与特征画像">
        <div class="section-card">
          <h3>用户画像雷达图</h3>
          <RadarChart :option="radarChartOption" :loading="radarLoading" />
        </div>

        <div class="section-card">
          <h3>用户行为转化漏斗</h3>
          <FunnelChart :option="funnelChartOption" :loading="chartsLoading" />
        </div>

        <div class="section-card">
          <h3>用户行为分布</h3>
          <BarChart :option="behaviorChartOption" :loading="chartsLoading" />
        </div>
      </aside>

      <section class="center-core" role="region" aria-label="实时推荐业务核心">
        <div class="search-bar">
          <el-select 
            v-model="queryUserId" 
            placeholder="请选择用户ID" 
            filterable
            style="flex: 1"
            :loading="userListLoading"
          >
            <el-option
              v-for="user in userList"
              :key="user"
              :label="user"
              :value="user"
            />
          </el-select>
          <el-button type="primary" @click="queryRecommendations" :loading="queryLoading">查询</el-button>
        </div>

        <div class="recommend-table-container">
          <div class="table-header">
            <span class="th">排名</span>
            <span class="th">商品ID</span>
            <span class="th">综合得分</span>
            <span class="th">预测概率</span>
            <span class="th">热度</span>
          </div>
          <TransitionGroup name="rank" tag="div" class="table-body">
            <div v-for="item in recommendations" :key="item.itemId" class="table-row">
              <span class="td rank-cell">{{ item.rankNo }}</span>
              <span class="td">{{ item.itemId }}</span>
              <span class="td score-cell">{{ formatNumber(item.finalScore) }}</span>
              <span class="td prob-cell">{{ formatNumber(item.predictScore) }}</span>  <!-- 原 predictionProbability → predictScore -->
              <span :class="['td', 'popularity', getPopularityClass(item.popularity)]">{{ item.popularity }}</span>
            </div>
          </TransitionGroup>
          <div v-if="recommendations.length === 0 && !queryLoading" class="empty-state">
            <span>暂无数据</span>
          </div>
        </div>

        <div class="user-info-panel" v-if="userRadarData">
          <h4>用户 {{ userRadarData.userId }} 画像详情</h4>
          <div class="info-grid">
            <div class="info-item"><span class="label">浏览量</span><span class="value">{{ userRadarData.pvCount }}</span></div>
            <div class="info-item"><span class="label">购买量</span><span class="value">{{ userRadarData.buyCount }}</span></div>
            <div class="info-item"><span class="label">加购量</span><span class="value">{{ userRadarData.cartCount }}</span></div>
            <div class="info-item"><span class="label">收藏量</span><span class="value">{{ userRadarData.favCount }}</span></div>
            <div class="info-item"><span class="label">购买率</span><span class="value">{{ formatPercent(userRadarData.buyRate) }}</span></div>
            <div class="info-item"><span class="label">活跃度</span><span class="value">{{ formatPercent(userRadarData.activeScore) }}</span></div>
          </div>
        </div>
      </section>

      <aside class="right-sidebar" role="region" aria-label="全局商品热度与效果评估">
        <div class="section-card">
          <h3>TOP10热门商品</h3>
          <BarChart :option="top10ChartOption" :loading="chartsLoading" />
        </div>

        <div class="section-card">
          <h3>推荐预测统计</h3>
          <PieChart :option="predictionChartOption" :loading="chartsLoading" />
        </div>

        <div class="section-card">
          <h3>CTR & CVR 双轴趋势</h3>
          <BarChart :option="ctrCvrDualAxisOption" :loading="chartsLoading" />
        </div>
      </aside>
    </main>

    <footer class="bottom-base" role="region" aria-label="系统运行状态监控">
      <h3>实时推荐数量趋势</h3>
      <LineChart :option="countTrendChartOption" :loading="chartsLoading" />
    </footer>

    <!-- 人工录入抽屉组件 -->
    <ManualInputDrawer />
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted, onUnmounted, watch } from 'vue'
import { Loading, Search } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import BarChart from './components/BarChart.vue'
import LineChart from './components/LineChart.vue'
import PieChart from './components/PieChart.vue'
import FunnelChart from './components/FunnelChart.vue'
import RadarChart from './components/RadarChart.vue'
import LiquidChart from './components/LiquidChart.vue'
import ManualInputDrawer from './components/ManualInputDrawer.vue'
import { api } from './services/api.js'
import { throttle, formatNumber } from './utils/index.js'

const particleCanvas = ref(null)
let animationId = null
let particles = []


const timeRange = ref([])
const isDefaultRange = ref(true)   // 新增：标记是否使用默认"今天"时间段
const timeLabel = ref('')
const queryUserId = ref(1001)
const recommendations = ref([])
const userRadarData = ref(null)
const prAucData = ref(null)              // 【新增】PR-AUC数据
const ctrCvrData = ref(null)             // 【新增】最新CTR/CVR
const chartsLoading = ref(false)
const queryLoading = ref(false)
const radarLoading = ref(false)
const isFlipping = ref(false)
const displayRecommendCount = ref(0)
const userList = ref([])
const userListLoading = ref(false)
let refreshTimer = null
const behaviorChartData = ref({ pv: 0, cart: 0, fav: 0, buy: 0 })
const funnelChartData = ref({ pv: 0, cart: 0, fav: 0, buy: 0 })
const predictionData = ref({ recommend: 0, notRecommend: 0 })
const ctrCvrTrendData = ref([])
const top10Data = ref([])
const countTrendData = ref([])

const liquidOption = (type) => {
  const values = {
    accuracy: prAucData.value?.prAuc || 0,     // ← 改为 prAucData
    ctr: ctrCvrData.value?.ctr || 0,            // ← 改为 ctrCvrData
    cvr: ctrCvrData.value?.cvr || 0             // ← 改为 ctrCvrData
  }
  const colors = {
    accuracy: [{ offset: 0, color: '#667eea' }, { offset: 1, color: '#764ba2' }],
    ctr: [{ offset: 0, color: '#f093fb' }, { offset: 1, color: '#f5576c' }],
    cvr: [{ offset: 0, color: '#43e97b' }, { offset: 1, color: '#38ef7d' }]
  }
  return {
    series: [{
      type: 'liquidFill',
      data: [values[type]],
      radius: '85%',
      center: ['50%', '50%'],
      shape: 'circle',
      color: [{ type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: colors[type] }],
      label: { show: false },
      outline: { show: false },
      backgroundStyle: { color: 'transparent' },
      itemStyle: { opacity: 0.85 }
    }]
  }
}

const behaviorChartOption = computed(() => ({
  tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
  grid: { left: '5%', right: '5%', bottom: '15%', top: '8%', containLabel: true },
  xAxis: { type: 'category', data: ['浏览', '加购', '收藏', '购买'], axisLine: { lineStyle: { color: '#aaa' } }, axisLabel: { color: '#ccc', fontSize: 11 } },
  yAxis: { type: 'value', axisLine: { lineStyle: { color: '#aaa' } }, axisLabel: { color: '#ccc', fontSize: 11 }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } } },
  series: [{
    type: 'bar',
    data: [behaviorChartData.value?.pv || 0, behaviorChartData.value?.cart || 0, behaviorChartData.value?.fav || 0, behaviorChartData.value?.buy || 0],
    barWidth: '50%',
    itemStyle: { borderRadius: [4, 4, 0, 0], color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: '#667eea' }, { offset: 1, color: '#764ba2' }] } }
  }]
}))

const predictionChartOption = computed(() => ({
  tooltip: { 
    trigger: 'item', 
    formatter: '{b}: {c} ({d}%)',
    backgroundColor: 'rgba(26, 26, 46, 0.9)',
    borderColor: 'rgba(102, 126, 234, 0.5)',
    borderWidth: 1,
    textStyle: { color: '#fff', fontSize: 13 }
  },
  series: [{
    type: 'pie',
    radius: ['40%', '70%'],
    center: ['50%', '50%'],
    startAngle: 90,
    itemStyle: { 
      borderRadius: 12, 
      borderColor: '#1a1a2e', 
      borderWidth: 3,
      shadowColor: 'rgba(102, 126, 234, 0.4)',
      shadowBlur: 15,
      shadowOffsetX: 0,
      shadowOffsetY: 5
    },
    label: { 
      show: true, 
      formatter: '{b}\n{d}%', 
      color: '#e0e0e0', 
      fontSize: 13, 
      fontWeight: 'bold',
      position: 'outside',
      alignTo: 'labelLine',
      distance: 15
    },
    labelLine: { 
      length: 15, 
      length2: 10,
      lineStyle: {
        color: 'rgba(102, 126, 234, 0.6)',
        width: 2
      }
    },
    emphasis: {
      scale: true,
      scaleSize: 10,
      itemStyle: {
        shadowBlur: 25,
        shadowOffsetX: 0,
        shadowOffsetY: 10
      },
      label: {
        fontSize: 15,
        fontWeight: 'bold'
      }
    },
    animationType: 'scale',
    animationEasing: 'elasticOut',
    animationDelay: () => Math.random() * 200,
    data: [
      { 
        value: predictionData.value?.recommend || 0, 
        name: '推荐商品', 
        itemStyle: { 
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 1, y2: 1,
            colorStops: [
              { offset: 0, color: '#667eea' },
              { offset: 0.5, color: '#764ba2' },
              { offset: 1, color: '#f093fb' }
            ]
          }
        }
      },
      { 
        value: predictionData.value?.notRecommend || 0, 
        name: '未推荐商品',
        itemStyle: { 
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 1, y2: 1,
            colorStops: [
              { offset: 0, color: '#4a4a6a' },
              { offset: 1, color: '#3a3a5a' }
            ]
          }
        }
      }
    ]
  }]
}))

const ctrCvrDualAxisOption = computed(() => ({
  tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
  legend: { data: ['CTR', 'CVR'], textStyle: { color: '#ccc' }, bottom: 0 },
  grid: { left: '3%', right: '4%', bottom: '15%', top: '10%', containLabel: true },
  xAxis: { type: 'category', data: ctrCvrTrendData.value?.map(d => d.time?.substring(11, 19)) || [], axisLine: { lineStyle: { color: '#aaa' } }, axisLabel: { color: '#ccc', rotate: 30 } },
  yAxis: [
    { type: 'value', name: 'CTR', min: 0, max: 0.3, axisLine: { lineStyle: { color: '#f093fb' } }, axisLabel: { color: '#ccc', formatter: v => (v * 100).toFixed(0) + '%' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } } },
    { type: 'value', name: 'CVR', min: 0, max: 0.1, axisLine: { lineStyle: { color: '#43e97b' } }, axisLabel: { color: '#ccc', formatter: v => (v * 100).toFixed(1) + '%' }, splitLine: { show: false } }
  ],
  series: [
    { name: 'CTR', type: 'bar', barWidth: '30%', data: ctrCvrTrendData.value?.map(d => d.ctr) || [], itemStyle: { borderRadius: [4, 4, 0, 0], color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: '#f093fb' }, { offset: 1, color: '#f5576c' }] } } },
    { name: 'CVR', type: 'line', yAxisIndex: 1, smooth: true, symbol: 'circle', symbolSize: 6, lineStyle: { color: '#43e97b', width: 2 }, itemStyle: { color: '#43e97b' }, data: ctrCvrTrendData.value?.map(d => d.cvr) || [] }
  ]
}))

const funnelChartOption = computed(() => {
  const data = funnelChartData.value || {}
  const total = (data.pv || 0) + (data.cart || 0) + (data.fav || 0) + (data.buy || 0) || 1
  
  const option = {
    tooltip: { 
    trigger: 'item', 
    formatter: (params) => {
      const rate = ((params.value / total) * 100).toFixed(2)
      return `${params.name}: ${params.value.toLocaleString()} (${rate}%)`
    },
    backgroundColor: 'rgba(26, 26, 46, 0.95)',
    borderColor: 'rgba(102, 126, 234, 0.6)',
    borderWidth: 1,
    padding: [12, 16],
    textStyle: { color: '#fff', fontSize: 14 }
  },
  series: [{
    name: '转化漏斗',
    type: 'funnel',
    left: '8%', top: 10, bottom: 10, width: '50%', height: '85%',
    min: 0, max: funnelChartData.value?.pv || 100,
    sort: 'descending', gap: 4,
    label: { 
      show: true, 
      position: 'right', 
      formatter: (params) => {
        const rate = ((params.value / total) * 100).toFixed(1)
        return `{name|${params.name}}{value|${params.value.toLocaleString()}}\n{rate|占比: ${rate}%}`
      },
      color: '#fff', 
      fontSize: 13,
      fontWeight: 'bold',
      align: 'left',
      rich: {
        name: {
          fontSize: 14,
          fontWeight: 'bold',
          color: '#e0e0e0',
          padding: [0, 0, 4, 0]
        },
        value: {
          fontSize: 16,
          fontWeight: 'bold',
          color: '#667eea',
          textShadowColor: 'rgba(102, 126, 234, 0.6)',
          textShadowBlur: 10
        },
        rate: {
          fontSize: 12,
          color: '#888',
          padding: [4, 0, 0, 0]
        }
      }
    },
    labelLine: { 
      show: true,
      length: 20,
      length2: 15,
      smooth: true,
      lineStyle: { 
        color: 'rgba(102, 126, 234, 0.8)', 
        width: 2,
        type: 'dashed'
      }
    },
    itemStyle: { 
      borderColor: '#1a1a2e', 
      borderWidth: 3,
      borderRadius: [12, 12, 0, 0],
      shadowColor: 'rgba(102, 126, 234, 0.4)',
      shadowBlur: 20,
      shadowOffsetX: 0,
      shadowOffsetY: 8
    },
    emphasis: {
      scale: true,
      scaleSize: 5,
      label: {
        fontSize: 15,
        fontWeight: 'bold'
      },
      itemStyle: {
        shadowBlur: 35,
        shadowOffsetY: 15
      }
    },
    animationType: 'scale',
    animationEasing: 'elasticOut',
    animationDelay: (idx) => idx * 150,
    data: [
      { 
        value: funnelChartData.value?.pv || 0, 
        name: '浏览', 
        itemStyle: { 
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: '#667eea' },
              { offset: 0.5, color: '#764ba2' },
              { offset: 1, color: '#8b5cf6' }
            ]
          },
          shadowColor: 'rgba(102, 126, 234, 0.5)',
          shadowBlur: 25
        }
      },
      { 
        value: funnelChartData.value?.cart || 0, 
        name: '加购', 
        itemStyle: { 
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: '#f093fb' },
              { offset: 0.5, color: '#f5576c' },
              { offset: 1, color: '#ec4899' }
            ]
          },
          shadowColor: 'rgba(240, 147, 251, 0.5)',
          shadowBlur: 25
        }
      },
      { 
        value: funnelChartData.value?.fav || 0, 
        name: '收藏', 
        itemStyle: { 
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: '#43e97b' },
              { offset: 0.5, color: '#38ef7d' },
              { offset: 1, color: '#34d399' }
            ]
          },
          shadowColor: 'rgba(67, 233, 123, 0.5)',
          shadowBlur: 25
        }
      },
      { 
        value: funnelChartData.value?.buy || 0, 
        name: '购买', 
        itemStyle: { 
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: '#f5576c' },
              { offset: 0.5, color: '#f093fb' },
              { offset: 1, color: '#f43f5e' }
            ]
          },
          shadowColor: 'rgba(245, 87, 108, 0.6)',
          shadowBlur: 30,
          borderRadius: [6, 6, 0, 0]
        }
      }
    ]
  }]
  }
  return option
})
const timeShortcuts = [
  { text: '今天', value: () => {
      const end = new Date()
      const start = new Date(end.getFullYear(), end.getMonth(), end.getDate(), 0, 0, 0)
      return [start, end]
    }},
  { text: '最近2小时', value: () => {
      const end = new Date()
      const start = new Date(end.getTime() - 2 * 60 * 60 * 1000)
      return [start, end]
    }},
  { text: '最近6小时', value: () => {
      const end = new Date()
      const start = new Date(end.getTime() - 6 * 60 * 60 * 1000)
      return [start, end]
    }},
  { text: '最近24小时', value: () => {
      const end = new Date()
      const start = new Date(end.getTime() - 24 * 60 * 60 * 1000)
      return [start, end]
    }}
]
const disabledDate = (time) => {
  return time.getTime() > Date.now()
}
const getNowStr = () => {
  const d = new Date()
  const pad = (n) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
}
const formatTime = (t) => {
  if (!t) return ''
  // 只取 HH:mm
  return t.substring(11, 16)
}
const onTimeRangeChange = () => {
  if (!timeRange.value || timeRange.value.length !== 2) {
    initTimeRange()
  } else {
    isDefaultRange.value = false         // ← 用户手动选择了时间，标记为非默认
  }
  timeLabel.value = `${formatTime(timeRange.value[0])} ~ ${timeRange.value[1] === getNowStr() ? '现在' : formatTime(timeRange.value[1])}`
  fetchData()
}
const initTimeRange = () => {
  isDefaultRange.value = true            // ← 标记为默认范围
  const now = new Date()
  const start = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0)
  timeRange.value = [
    `${start.getFullYear()}-${String(start.getMonth()+1).padStart(2,'0')}-${String(start.getDate()).padStart(2,'0')} 00:00:00`,
    getNowStr()
  ]
  timeLabel.value = `${formatTime(timeRange.value[0])} ~ 现在`
}

const top10ChartOption = computed(() => ({
  tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
  grid: { left: '5%', right: '10%', bottom: '3%', top: '5%', containLabel: true },
  xAxis: { type: 'value', axisLine: { lineStyle: { color: '#aaa' } }, axisLabel: { color: '#ccc' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } } },
  yAxis: { type: 'category', data: top10Data.value?.map(d => d.itemId).reverse() || [], axisLine: { lineStyle: { color: '#aaa' } }, axisLabel: { color: '#ccc', fontSize: 10 } },
  series: [{
    type: 'bar',
    barWidth: '50%',
    data: top10Data.value?.map(d => d.pvCount || 1000).reverse() || [],
    itemStyle: { borderRadius: [0, 4, 4, 0], color: { type: 'linear', x: 0, y: 0, x2: 1, y2: 0, colorStops: [{ offset: 0, color: '#4facfe' }, { offset: 1, color: '#00f2fe' }] } }
  }]
}))

const countTrendChartOption = computed(() => ({
  tooltip: { trigger: 'axis' },
  grid: { left: '2%', right: '2%', bottom: '8%', top: '8%', containLabel: true },
  xAxis: { type: 'category', data: countTrendData.value?.map(d => d.time?.substring(11, 16)) || [], axisLine: { lineStyle: { color: '#aaa' } }, axisLabel: { color: '#ccc', rotate: 45 }, boundaryGap: false },
  yAxis: { type: 'value', axisLine: { lineStyle: { color: '#aaa' } }, axisLabel: { color: '#ccc' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } } },
  series: [{
    type: 'line',
    smooth: true,
    symbol: 'circle',
    symbolSize: 6,
    lineStyle: { color: '#43e97b', width: 2 },
    areaStyle: { opacity: 0.4, color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: 'rgba(67, 233, 123, 0.6)' }, { offset: 1, color: 'rgba(67, 233, 123, 0.05)' }] } },
    itemStyle: { color: '#43e97b' },
    data: countTrendData.value?.map(d => d.count) || []
  }]
}))

const radarChartOption = computed(() => {
  const data = userRadarData.value || {}
  return {
    tooltip: { 
      trigger: 'item',
      confine: true,
      backgroundColor: 'rgba(26, 26, 46, 0.95)',
      borderColor: 'rgba(102, 126, 234, 0.6)',
      borderWidth: 1,
      textStyle: { color: '#fff', fontSize: 12 },
      formatter: (params) => {
        const indicators = ['浏览量', '购买量', '加购量', '收藏量', '购买率', '活跃度']
        let result = `<div style="font-weight:bold;margin-bottom:8px;">用户画像</div>`
        params.value.forEach((val, idx) => {
          const name = indicators[idx]
          const value = name.includes('率') || name.includes('度') ? val.toFixed(2) : val
          result += `<div style="display:flex;justify-content:space-between;min-width:100px;">
            <span>${name}:</span>
            <span style="color:#667eea;font-weight:bold;margin-left:12px;">${value}</span>
          </div>`
        })
        return result
      }
    },
    legend: { show: false },
    radar: {
      indicator: [
        { name: '浏览量', max: 300 },
        { name: '购买量', max: 50 },
        { name: '加购量', max: 60 },
        { name: '收藏量', max: 40 },
        { name: '购买率', max: 1 },
        { name: '活跃度', max: 1 }
      ],
      center: ['50%', '50%'],
      radius: '50%',
      axisName: { color: '#ccc', fontSize: 11 },
      splitArea: { areaStyle: { color: ['rgba(102, 126, 234, 0.05)', 'rgba(102, 126, 234, 0.1)'] } },
      splitLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } },
      axisLine: { lineStyle: { color: 'rgba(255,255,255,0.2)' } }
    },
    series: [{
      type: 'radar',
      data: [{
        value: [data.pvCount || 0, data.buyCount || 0, data.cartCount || 0, data.favCount || 0, data.buyRate || 0, data.activeScore || 0],
        name: '用户画像',
        areaStyle: { color: 'rgba(102, 126, 234, 0.3)' },
        lineStyle: { color: '#667eea', width: 2 },
        itemStyle: { color: '#667eea' }
      }]
    }]
  }
})

const formatPercent = (value) => {
  return ((value || 0) * 100).toFixed(2) + '%'
}

const getPopularityClass = (p) => p === '高' ? 'high' : p === '中' ? 'medium' : 'low'

const fetchData = async () => {
  chartsLoading.value = true
  try {
    let startTime, endTime
    if (timeRange.value && timeRange.value.length === 2) {
      startTime = timeRange.value[0]
      // ===== 【新增】如果是默认范围，结束时间自动更新为当前时间 =====
      if (isDefaultRange.value) {
        endTime = getNowStr()
        // 同步更新 timeRange 显示
        timeRange.value = [startTime, endTime]
        timeLabel.value = `${formatTime(startTime)} ~ 现在`
      } else {
        endTime = timeRange.value[1]
      }
    } else {
      const d = new Date()
      startTime = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')} 00:00:00`
      endTime = getNowStr()
    }

    const [
      behaviorRes, funnelRes, predictionRes,
      prAucRes, ctrCvrRes,
      ctrCvrTrendRes, top10Res, countTrendRes
    ] = await Promise.all([
      api.getBehaviorDistribution(),
      api.getBehaviorFunnel(),
      api.getPredictionStats(),
      api.getPrAuc(),
      api.getCtrCvr(startTime, endTime),
      api.getCtrCvrTrend(startTime, endTime),
      api.getTop10Items(),
      api.getRecommendCountTrend(startTime, endTime)
    ])

    behaviorChartData.value = behaviorRes.data
    funnelChartData.value = funnelRes.data
    predictionData.value = predictionRes.data
    prAucData.value = prAucRes.data
    ctrCvrData.value = ctrCvrRes.data
    ctrCvrTrendData.value = ctrCvrTrendRes.data
    top10Data.value = top10Res.data
    countTrendData.value = countTrendRes.data

    const newCount = ctrCvrRes.data?.recommendCount || 0
    if (newCount !== displayRecommendCount.value) {
      isFlipping.value = true
      setTimeout(() => {
        displayRecommendCount.value = newCount
        isFlipping.value = false
      }, 300)
    }
  } catch (error) {
    console.error('Fetch error:', error)
  } finally {
    chartsLoading.value = false
  }
}


const queryRecommendations = async () => {
  if (!queryUserId.value) {
    ElMessage.warning('请输入用户ID')
    return
  }
  queryLoading.value = true
  try {
    const [recRes, radarRes] = await Promise.all([
      api.getLatestRecommendations(queryUserId.value),
      api.getUserRadar(queryUserId.value)
    ])
    recommendations.value = recRes.data
    userRadarData.value = radarRes.data
  } catch (error) {
    console.error('Query error:', error)
    ElMessage.error('查询失败')
  } finally {
    queryLoading.value = false
  }
}

const submitBehavior = async () => {
  if (!behaviorFormRef.value) return
  await behaviorFormRef.value.validate(async (valid) => {
    if (!valid) return
    submitLoading.value = true
    try {
      await api.insertBehavior({ ...behaviorData.value, timestamp: Math.floor(Date.now() / 1000) })
      ElMessage.success('行为录入成功！')
      fetchData()
    } catch (error) {
      console.error('Submit error:', error)
      ElMessage.error('录入失败')
    } finally {
      submitLoading.value = false
    }
  })
}

const resetForm = () => {
  if (behaviorFormRef.value) behaviorFormRef.value.resetFields()
}

const initParticles = () => {
  const canvas = particleCanvas.value
  if (!canvas) return
  
  const ctx = canvas.getContext('2d')
  const resizeCanvas = () => {
    canvas.width = window.innerWidth
    canvas.height = window.innerHeight
  }
  resizeCanvas()
  window.addEventListener('resize', resizeCanvas)

  const colors = ['#667eea', '#764ba2', '#f093fb', '#43e97b', '#4facfe']

  class Particle {
    constructor() {
      this.reset()
    }
    reset() {
      this.x = Math.random() * canvas.width
      this.y = Math.random() * canvas.height
      this.vx = (Math.random() - 0.5) * 0.8
      this.vy = (Math.random() - 0.5) * 0.8
      this.radius = Math.random() * 3 + 1
      this.opacity = Math.random() * 0.6 + 0.3
      this.color = colors[Math.floor(Math.random() * colors.length)]
      this.pulsePhase = Math.random() * Math.PI * 2
      this.pulseSpeed = Math.random() * 0.02 + 0.01
    }
    update() {
      this.x += this.vx
      this.y += this.vy
      this.pulsePhase += this.pulseSpeed
      
      if (this.x < 0 || this.x > canvas.width) this.vx *= -1
      if (this.y < 0 || this.y > canvas.height) this.vy *= -1
    }
    draw() {
      const pulseRadius = this.radius * (1 + Math.sin(this.pulsePhase) * 0.3)
      const pulseOpacity = this.opacity * (1 + Math.sin(this.pulsePhase) * 0.2)
      
      const hexToRgba = function(hex, alpha) {
        const r = parseInt(hex.slice(1, 3), 16)
        const g = parseInt(hex.slice(3, 5), 16)
        const b = parseInt(hex.slice(5, 7), 16)
        return 'rgba(' + r + ', ' + g + ', ' + b + ', ' + alpha + ')'
      }
      
      const gradient = ctx.createRadialGradient(this.x, this.y, 0, this.x, this.y, pulseRadius * 2)
      gradient.addColorStop(0, hexToRgba(this.color, pulseOpacity))
      gradient.addColorStop(0.5, hexToRgba(this.color, pulseOpacity * 0.5))
      gradient.addColorStop(1, 'rgba(102, 126, 234, 0)')
      
      ctx.beginPath()
      ctx.arc(this.x, this.y, pulseRadius * 2, 0, Math.PI * 2)
      ctx.fillStyle = gradient
      ctx.fill()
      
      ctx.beginPath()
      ctx.arc(this.x, this.y, pulseRadius, 0, Math.PI * 2)
      ctx.fillStyle = hexToRgba(this.color, pulseOpacity)
      ctx.fill()
    }
  }

  for (let i = 0; i < 120; i++) {
    particles.push(new Particle())
  }

  const animate = () => {
    ctx.clearRect(0, 0, canvas.width, canvas.height)
    particles.forEach(p => {
      p.update()
      p.draw()
    })
    particles.forEach((p1, i) => {
      particles.slice(i + 1).forEach(p2 => {
        const dx = p1.x - p2.x
        const dy = p1.y - p2.y
        const dist = Math.sqrt(dx * dx + dy * dy)
        if (dist < 150) {
          const gradient = ctx.createLinearGradient(p1.x, p1.y, p2.x, p2.y)
          gradient.addColorStop(0, `rgba(102, 126, 234, ${0.15 * (1 - dist / 150)})`)
          gradient.addColorStop(1, `rgba(240, 147, 251, ${0.15 * (1 - dist / 150)})`)
          ctx.beginPath()
          ctx.moveTo(p1.x, p1.y)
          ctx.lineTo(p2.x, p2.y)
          ctx.strokeStyle = gradient
          ctx.lineWidth = 0.8
          ctx.stroke()
        }
      })
    })
    animationId = requestAnimationFrame(animate)
  }
  animate()
}

const handleResize = throttle(() => {
  window.dispatchEvent(new Event('resize'))
}, 200)

const loadUserList = async () => {
  if (userList.value.length > 0) return
  userListLoading.value = true
  try {
    const res = await api.getUserList()
    if (res.code === 200) {
      userList.value = res.data
    }
  } catch (error) {
    console.error('加载用户列表失败:', error)
  } finally {
    userListLoading.value = false
  }
}

onMounted(() => {
  // 页面加载时获取全体平均画像
  api.getUserRadar(null).then(res => {
    radarData.value = res.data
  })
  initParticles()
  initTimeRange()    // ← 新增：初始化时间范围
  fetchData()
  queryRecommendations()
  loadUserList()
  refreshTimer = setInterval(fetchData, 3000)
  window.addEventListener('resize', handleResize)
})


onUnmounted(() => {
  if (refreshTimer) clearInterval(refreshTimer)
  if (animationId) cancelAnimationFrame(animationId)
  window.removeEventListener('resize', handleResize)
})
</script>

<style scoped>
.dashboard {
  min-height: 100vh;
  background: linear-gradient(135deg, #0d0d1a 0%, #1a1a2e 50%, #16213e 100%);
  color: #fff;
  position: relative;
  overflow: hidden;
}

.dashboard::before {
  content: '';
  position: fixed;
  top: -50%;
  left: -50%;
  width: 200%;
  height: 200%;
  background: radial-gradient(circle at 30% 20%, rgba(102, 126, 234, 0.15) 0%, transparent 50%),
              radial-gradient(circle at 70% 80%, rgba(118, 75, 162, 0.1) 0%, transparent 50%),
              radial-gradient(circle at 50% 50%, rgba(240, 147, 251, 0.05) 0%, transparent 70%);
  animation: gradient-shift 20s ease infinite;
  pointer-events: none;
  z-index: 0;
}

@keyframes gradient-shift {
  0%, 100% { transform: rotate(0deg) scale(1); opacity: 1; }
  50% { transform: rotate(180deg) scale(1.1); opacity: 0.8; }
}

.particle-bg {
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  pointer-events: none;
  z-index: 0;
}

.top-banner {
  padding: 15px 20px;
  background: rgba(0, 0, 0, 0.4);
  border-bottom: 1px solid rgba(255, 255, 255, 0.1);
  position: relative;
  z-index: 10;
  backdrop-filter: blur(10px);
}

.top-banner::before {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  height: 1px;
  background: linear-gradient(90deg, transparent, #667eea, transparent);
  animation: shimmer 3s ease-in-out infinite;
}

@keyframes shimmer {
  0%, 100% { opacity: 0.3; }
  50% { opacity: 1; }
}

.top-banner h1 {
  margin: 0 0 15px 0;
  font-size: 24px;
  text-align: center;
  background: linear-gradient(90deg, #667eea, #764ba2, #f093fb, #667eea);
  background-size: 300% 100%;
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
  animation: title-gradient 4s ease infinite;
}

@keyframes title-gradient {
  0%, 100% { background-position: 0% 50%; }
  50% { background-position: 100% 50%; }
}

.banner-content {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 25px;
  flex-wrap: wrap;
}

.metric-card {
  width: 120px;
  height: 120px;
  background: rgba(255, 255, 255, 0.05);
  border-radius: 12px;
  padding: 10px;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  border: 1px solid rgba(255, 255, 255, 0.1);
  transition: all 0.3s ease;
  position: relative;
  overflow: hidden;
}

.metric-card::before {
  content: '';
  position: absolute;
  top: -50%;
  left: -50%;
  width: 200%;
  height: 200%;
  background: conic-gradient(from 0deg, transparent, rgba(102, 126, 234, 0.2), transparent, rgba(102, 126, 234, 0.2));
  animation: rotate-border 8s linear infinite;
}

@keyframes rotate-border {
  100% { transform: rotate(360deg); }
}

.metric-card::after {
  content: '';
  position: absolute;
  inset: 1px;
  background: rgba(10, 10, 20, 0.8);
  border-radius: 11px;
  z-index: 0;
}

.metric-card:hover {
  transform: translateY(-5px) scale(1.02);
  box-shadow: 0 10px 40px rgba(102, 126, 234, 0.3),
              0 0 20px rgba(102, 126, 234, 0.2),
              inset 0 0 30px rgba(102, 126, 234, 0.1);
}

.metric-card > div {
  position: relative;
  z-index: 1;
}

.metric-card > div:first-child {
  width: 80px;
  height: 80px;
  animation: pulse 2s ease-in-out infinite;
}

@keyframes pulse {
  0%, 100% { transform: scale(1); }
  50% { transform: scale(1.05); }
}

.metric-label {
  font-size: 12px;
  color: #aaa;
  margin-top: 5px;
}

.metric-value {
  font-size: 18px;
  font-weight: bold;
  color: #667eea;
  text-shadow: 0 0 10px rgba(102, 126, 234, 0.5);
  animation: value-glow 2s ease-in-out infinite;
}

@keyframes value-glow {
  0%, 100% { text-shadow: 0 0 10px rgba(102, 126, 234, 0.5); }
  50% { text-shadow: 0 0 20px rgba(102, 126, 234, 0.8), 0 0 30px rgba(102, 126, 234, 0.4); }
}

.flip-card {
  width: 150px;
  height: 120px;
  background: rgba(255, 255, 255, 0.05);
  border-radius: 12px;
  padding: 10px;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  border: 1px solid rgba(67, 233, 123, 0.3);
  position: relative;
  overflow: hidden;
  transition: all 0.3s ease;
}

.flip-card::before {
  content: '';
  position: absolute;
  top: 0;
  left: -100%;
  width: 100%;
  height: 100%;
  background: linear-gradient(90deg, transparent, rgba(67, 233, 123, 0.2), transparent);
  animation: scan-line 3s linear infinite;
}

@keyframes scan-line {
  100% { left: 100%; }
}

.flip-card:hover {
  transform: translateY(-5px) scale(1.02);
  box-shadow: 0 10px 40px rgba(67, 233, 123, 0.3),
              0 0 30px rgba(67, 233, 123, 0.2);
}

.flip-label {
  font-size: 12px;
  color: #aaa;
  margin-bottom: 10px;
}

.flip-value {
  font-size: 32px;
  font-weight: bold;
  color: #43e97b;
  font-family: 'Courier New', monospace;
  text-shadow: 0 0 15px rgba(67, 233, 123, 0.6);
  animation: count-glow 2s ease-in-out infinite;
}

@keyframes count-glow {
  0%, 100% { text-shadow: 0 0 15px rgba(67, 233, 123, 0.6); }
  50% { text-shadow: 0 0 25px rgba(67, 233, 123, 0.9), 0 0 40px rgba(67, 233, 123, 0.5); }
}

.flipping {
  animation: flip 0.5s cubic-bezier(0.68, -0.55, 0.265, 1.55);
}

@keyframes flip {
  0% { transform: scaleY(1) rotateX(0deg); }
  50% { transform: scaleY(0.1) rotateX(90deg); }
  100% { transform: scaleY(1) rotateX(0deg); }
}

.main-layout {
  display: flex;
  height: calc(100vh - 200px);
  padding: 15px;
  gap: 15px;
  position: relative;
  z-index: 10;
}

.left-sidebar {
  width: 30%;
  display: flex;
  flex-direction: column;
  gap: 15px;
  overflow-y: auto;
  flex-shrink: 0;
  padding-right: 5px;
}

.center-core {
  width: 40%;
  display: flex;
  flex-direction: column;
  gap: 10px;
  overflow-y: auto;
  flex-shrink: 0;
}

.right-sidebar {
  width: 30%;
  display: flex;
  flex-direction: column;
  gap: 15px;
  overflow-y: auto;
  flex-shrink: 0;
}

.section-card {
  background: rgba(255, 255, 255, 0.03);
  backdrop-filter: blur(10px);
  border-radius: 16px;
  padding: 16px;
  border: 1px solid rgba(255, 255, 255, 0.08);
  flex: 1;
  overflow: hidden;
  position: relative;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
  min-height: 240px;
  display: flex;
  flex-direction: column;
}

.section-card > h3 {
  flex-shrink: 0;
}

.section-card > :deep(.radar-chart),
.section-card > :deep(.funnel-chart),
.section-card > :deep(.bar-chart),
.section-card > :deep(.pie-chart),
.section-card > :deep(.line-chart) {
  flex: 1;
  min-height: 200px;
}

.section-card::before {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  border-radius: 16px;
  padding: 1px;
  background: linear-gradient(135deg, rgba(102, 126, 234, 0.4), rgba(240, 147, 251, 0.2), rgba(67, 233, 123, 0.3));
  -webkit-mask: linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);
  mask: linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);
  -webkit-mask-composite: xor;
  mask-composite: exclude;
  animation: border-glow 4s ease-in-out infinite;
  pointer-events: none;
}

@keyframes border-glow {
  0%, 100% { opacity: 0.3; }
  50% { opacity: 0.8; }
}

.section-card:hover {
  transform: translateY(-5px) scale(1.01);
  box-shadow: 0 15px 40px rgba(102, 126, 234, 0.25),
              0 5px 15px rgba(0, 0, 0, 0.3),
              inset 0 0 20px rgba(102, 126, 234, 0.08);
  border-color: rgba(102, 126, 234, 0.3);
}

.section-card h3 {
  margin: 0 0 12px 0;
  font-size: 15px;
  font-weight: 600;
  color: #fff;
  text-shadow: 0 0 15px rgba(102, 126, 234, 0.6);
  position: relative;
  z-index: 1;
  letter-spacing: 0.5px;
}

.section-card h3::after {
  content: '';
  display: block;
  width: 40px;
  height: 3px;
  background: linear-gradient(90deg, #667eea, #f093fb, transparent);
  margin-top: 8px;
  border-radius: 2px;
  animation: underline-grow 2.5s ease-in-out infinite;
}

@keyframes underline-grow {
  0%, 100% { width: 30px; }
  50% { width: 60px; }
}

.behavior-form-card {
  padding: 12px;
  min-height: 280px;
  height: fit-content;
  position: relative;
  background: rgba(255, 255, 255, 0.05);
  border-radius: 12px;
  border: 1px solid rgba(255, 255, 255, 0.1);
  z-index: 1;
}

.behavior-form-card .form-content {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.behavior-form-card :deep(.el-form) {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.behavior-form-card :deep(.el-form-item) {
  margin-bottom: 0 !important;
}

.behavior-form-card :deep(.el-form-item__content) {
  width: 100%;
}

.behavior-form-card .form-actions {
  display: flex;
  gap: 10px;
  padding-top: 15px;
  border-top: 1px solid rgba(255, 255, 255, 0.1);
}

.behavior-form-card .form-actions :deep(.el-button) {
  flex: 1;
  height: 36px;
}

.behavior-form-card h3 {
  margin-bottom: 15px;
}

.search-bar {
  display: flex;
  gap: 10px;
  position: relative;
}

.search-input {
  flex: 1;
  position: relative;
}

.search-input::before {
  content: '';
  position: absolute;
  top: -2px;
  left: -2px;
  right: -2px;
  bottom: -2px;
  border-radius: 8px;
  background: linear-gradient(135deg, #667eea, #764ba2);
  opacity: 0;
  transition: opacity 0.3s ease;
  z-index: -1;
}

.search-input:focus-within::before {
  opacity: 1;
  animation: input-glow 2s ease-in-out infinite;
}

@keyframes input-glow {
  0%, 100% { opacity: 0.5; }
  50% { opacity: 1; }
}

.search-bar :deep(.el-button--primary) {
  position: relative;
  overflow: hidden;
  transition: all 0.3s ease;
}

.search-bar :deep(.el-button--primary):hover {
  transform: translateY(-2px);
  box-shadow: 0 5px 20px rgba(102, 126, 234, 0.4);
}

.search-bar :deep(.el-button--primary)::before {
  content: '';
  position: absolute;
  top: 0;
  left: -100%;
  width: 100%;
  height: 100%;
  background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.2), transparent);
  animation: button-shine 2s linear infinite;
}

@keyframes button-shine {
  100% { left: 100%; }
}

.recommend-table-container {
  flex: 1;
  background: rgba(255, 255, 255, 0.03);
  backdrop-filter: blur(8px);
  border-radius: 16px;
  border: 1px solid rgba(255, 255, 255, 0.08);
  overflow: hidden;
  display: flex;
  flex-direction: column;
}

.table-header {
  display: flex;
  background: linear-gradient(135deg, rgba(102, 126, 234, 0.25), rgba(240, 147, 251, 0.15));
  padding: 14px 18px;
  font-size: 13px;
  color: #fff;
  font-weight: 600;
  position: sticky;
  top: 0;
  letter-spacing: 0.5px;
  text-shadow: 0 0 10px rgba(102, 126, 234, 0.5);
}

.th {
  flex: 1;
  text-align: center;
}

.th:first-child { flex: 0.5; }
.th:last-child { flex: 0.5; }

.table-body {
  padding: 12px;
  flex: 1;
  overflow-y: auto;
}

.table-row {
  display: flex;
  padding: 10px 15px;
  background: rgba(255, 255, 255, 0.05);
  border-radius: 8px;
  margin-bottom: 8px;
  font-size: 13px;
  transition: all 0.3s ease;
  position: relative;
  overflow: hidden;
}

.table-row::before {
  content: '';
  position: absolute;
  left: 0;
  top: 0;
  bottom: 0;
  width: 3px;
  background: linear-gradient(180deg, #667eea, #764ba2);
  transform: scaleY(0);
  transition: transform 0.3s ease;
}

.table-row:hover::before {
  transform: scaleY(1);
}

.table-row:hover {
  background: rgba(102, 126, 234, 0.15);
  transform: translateX(8px);
  box-shadow: 0 4px 20px rgba(102, 126, 234, 0.2);
}

.td {
  flex: 1;
  text-align: center;
}

.td:first-child { flex: 0.5; }
.td:last-child { flex: 0.5; }

.rank-cell { color: #667eea; font-weight: bold; }
.score-cell { color: #43e97b; }
.prob-cell { color: #f093fb; }

.popularity {
  font-weight: bold;
  padding: 4px 12px;
  border-radius: 12px;
  text-transform: uppercase;
  font-size: 11px;
  position: relative;
  overflow: hidden;
}

.popularity::before {
  content: '';
  position: absolute;
  top: 0;
  left: -100%;
  width: 100%;
  height: 100%;
  background: linear-gradient(90deg, transparent, rgba(255,255,255,0.2), transparent);
  animation: shimmer 2s infinite;
}

@keyframes shimmer {
  100% { left: 100%; }
}

.popularity.high { 
  color: #fff; 
  background: linear-gradient(135deg, #ff4d4f, #ff7a45, #faad14);
  box-shadow: 0 0 15px rgba(255, 77, 79, 0.6), inset 0 0 20px rgba(255, 255, 255, 0.1);
}

.popularity.medium { 
  color: #fff; 
  background: linear-gradient(135deg, #20c997, #17a2b8, #138496);
  box-shadow: 0 0 12px rgba(32, 201, 151, 0.5), inset 0 0 15px rgba(255, 255, 255, 0.1);
}

.popularity.low { 
  color: #fff; 
  background: linear-gradient(135deg, #1890ff, #096dd9, #0050b3);
  box-shadow: 0 0 10px rgba(24, 144, 255, 0.4), inset 0 0 10px rgba(255, 255, 255, 0.1);
}

@keyframes pulse-high {
  0%, 100% { box-shadow: 0 0 15px rgba(255, 77, 79, 0.6); }
  50% { box-shadow: 0 0 25px rgba(255, 77, 79, 0.9), 0 0 40px rgba(255, 122, 69, 0.5); }
}

@keyframes pulse-medium {
  0%, 100% { box-shadow: 0 0 8px rgba(240, 147, 251, 0.4); }
  50% { box-shadow: 0 0 15px rgba(240, 147, 251, 0.6), 0 0 25px rgba(102, 126, 234, 0.3); }
}

.empty-state {
  display: flex;
  justify-content: center;
  align-items: center;
  padding: 40px;
  color: #aaa;
}

.user-info-panel {
  background: rgba(255, 255, 255, 0.03);
  backdrop-filter: blur(8px);
  border-radius: 16px;
  padding: 16px;
  border: 1px solid rgba(255, 255, 255, 0.08);
  position: relative;
  overflow: hidden;
  animation: panel-appear 0.5s ease-out;
}

@keyframes panel-appear {
  0% { opacity: 0; transform: translateY(20px); }
  100% { opacity: 1; transform: translateY(0); }
}

.user-info-panel::before {
  content: '';
  position: absolute;
  top: -50%;
  right: -50%;
  width: 100%;
  height: 100%;
  background: radial-gradient(circle, rgba(102, 126, 234, 0.12) 0%, transparent 70%);
  animation: float-bg 6s ease-in-out infinite;
}

@keyframes float-bg {
  0%, 100% { transform: translateX(0) translateY(0); }
  50% { transform: translateX(-20px) translateY(-20px); }
}

.user-info-panel h4 {
  margin: 0 0 14px 0;
  font-size: 14px;
  font-weight: 600;
  color: #fff;
  text-shadow: 0 0 15px rgba(102, 126, 234, 0.6);
  position: relative;
  z-index: 1;
  letter-spacing: 0.5px;
}

.info-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 12px;
  position: relative;
  z-index: 1;
}

.info-item {
  display: flex;
  justify-content: space-between;
  font-size: 13px;
  padding: 10px 12px;
  background: rgba(255, 255, 255, 0.04);
  border-radius: 10px;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
  border: 1px solid rgba(255, 255, 255, 0.03);
}

.info-item:hover {
  background: rgba(102, 126, 234, 0.1);
  transform: translateY(-2px);
  box-shadow: 0 4px 15px rgba(102, 126, 234, 0.15);
  border-color: rgba(102, 126, 234, 0.2);
}

.info-label {
  color: #aaa;
  font-weight: 500;
}

.info-value {
  color: #fff;
  font-weight: bold;
  text-shadow: 0 0 8px rgba(255, 255, 255, 0.2);
}

.bottom-base {
  padding: 18px 24px;
  background: rgba(0, 0, 0, 0.25);
  border-top: 1px solid rgba(255, 255, 255, 0.08);
  position: relative;
  z-index: 10;
  backdrop-filter: blur(12px);
}

.bottom-base::before {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  height: 2px;
  background: linear-gradient(90deg, transparent, #667eea, #f093fb, #43e97b, transparent);
  animation: bottom-shimmer 3.5s ease-in-out infinite;
}

@keyframes bottom-shimmer {
  0%, 100% { opacity: 0.25; }
  50% { opacity: 1; }
}

.bottom-base h3 {
  margin: 0 0 12px 0;
  font-size: 15px;
  font-weight: 600;
  color: #43e97b;
  text-shadow: 0 0 15px rgba(67, 233, 123, 0.6);
  letter-spacing: 0.5px;
}

.rank-enter-active {
  animation: bounce-in 0.5s cubic-bezier(0.68, -0.55, 0.265, 1.55);
}

.rank-leave-active {
  transition: all 0.3s ease-out;
}

.rank-enter-from {
  opacity: 0;
  transform: translateY(-20px);
}

.rank-leave-to {
  opacity: 0;
  transform: translateY(20px);
}

.rank-move {
  transition: transform 0.5s cubic-bezier(0.4, 0, 0.2, 1);
}

@keyframes bounce-in {
  0% { opacity: 0; transform: translateY(-30px) scale(0.8); }
  60% { transform: translateY(5px) scale(1.05); }
  100% { opacity: 1; transform: translateY(0) scale(1); }
}

@media (max-width: 1200px) {
  .main-layout {
    flex-direction: column;
    height: auto;
  }
  .left-sidebar, .center-core, .right-sidebar {
    width: 100%;
  }
  .right-sidebar {
    flex-direction: row;
    flex-wrap: wrap;
  }
  .right-sidebar .section-card {
    flex: 1;
    min-width: 300px;
  }
}

@media (max-width: 768px) {
  .top-banner h1 { font-size: 18px; }
  .banner-content { gap: 15px; }
  .metric-card, .flip-card { width: 100px; height: 100px; }
  .flip-value { font-size: 24px; }
  .info-grid { grid-template-columns: 1fr; }
}
.time-range-picker {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  margin-bottom: 15px;
  flex-wrap: wrap;
}
.time-label {
  font-size: 13px;
  color: #ccc;
  white-space: nowrap;
}
.time-hint {
  font-size: 12px;
  color: #888;
  white-space: nowrap;
}
.metric-sub-label {
  font-size: 10px;
  color: #888;
  margin-top: 3px;
  position: relative;
  z-index: 1;
  text-align: center;
}
</style>