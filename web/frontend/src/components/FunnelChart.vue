<template>
  <div class="funnel-chart" v-loading="loading" :element-loading-background="loadingBg">
    <div ref="chartRef" class="chart-container"></div>
    <div v-if="hasError" class="error-state">图表加载失败</div>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, watch } from 'vue'
import * as echarts from 'echarts/core'
import { FunnelChart } from 'echarts/charts'
import { TooltipComponent, LegendComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'

echarts.use([FunnelChart, TooltipComponent, LegendComponent, CanvasRenderer])

const props = defineProps({
  option: { type: Object, default: () => ({}) },
  loading: { type: Boolean, default: false },
  hasError: { type: Boolean, default: false }
})

const chartRef = ref(null)
let chartInstance = null
const loadingBg = 'rgba(26, 26, 46, 0.8)'

const initChart = () => {
  if (!chartRef.value) return
  chartInstance = echarts.init(chartRef.value, 'dark')
  updateChart()
}

const updateChart = () => {
  if (chartInstance && !props.hasError) {
    chartInstance.setOption(props.option, { notMerge: true })
  }
}

const resize = () => { chartInstance?.resize() }

watch(() => props.option, updateChart, { deep: true })

onMounted(() => { initChart(); window.addEventListener('resize', resize) })

onUnmounted(() => {
  window.removeEventListener('resize', resize)
  chartInstance?.dispose()
})
</script>

<style scoped>
.funnel-chart {
  width: 100%;
  height: 100%;
  position: relative;
  min-height: 180px;
}

.chart-container {
  width: 100%;
  height: 100%;
}

.error-state {
  display: flex;
  justify-content: center;
  align-items: center;
  height: 100%;
  color: #f5576c;
  font-size: 14px;
}
</style>