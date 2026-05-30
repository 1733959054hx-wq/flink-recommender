<template>
  <div ref="chartRef" class="chart-wrapper" v-loading="loading" :element-loading-background="loadingBg">
    <slot v-if="hasError" name="error">
      <div class="error-tip">图表加载失败</div>
    </slot>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, watch } from 'vue'
import * as echarts from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, LegendComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'

echarts.use([LineChart, GridComponent, TooltipComponent, LegendComponent, CanvasRenderer])

const props = defineProps({
  option: {
    type: Object,
    default: () => ({})
  },
  loading: {
    type: Boolean,
    default: false
  },
  hasError: {
    type: Boolean,
    default: false
  }
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
  if (chartInstance) {
    chartInstance.setOption(props.option, { notMerge: true })
  }
}

const resize = () => {
  chartInstance?.resize()
}

watch(() => props.option, updateChart, { deep: true })

onMounted(() => {
  initChart()
  window.addEventListener('resize', resize)
})

onUnmounted(() => {
  window.removeEventListener('resize', resize)
  chartInstance?.dispose()
})

defineExpose({ resize, updateChart })
</script>

<style scoped>
.chart-wrapper {
  width: 100%;
  height: 100%;
  min-height: 180px;
}
.error-tip {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: #f5576c;
}
</style>