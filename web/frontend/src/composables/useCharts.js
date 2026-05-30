import { ref, onMounted, onUnmounted, watch } from 'vue'
import * as echarts from 'echarts/core'
import { BarChart, LineChart, PieChart } from 'echarts/charts'
import {
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent
} from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'

echarts.use([
  BarChart,
  LineChart,
  PieChart,
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  CanvasRenderer
])

export function useChart(containerRef, theme = 'dark') {
  const chartInstance = ref(null)
  const isLoading = ref(false)
  const hasError = ref(false)

  const initChart = (option = {}) => {
    if (!containerRef.value) return null

    chartInstance.value = echarts.init(containerRef.value, theme)
    if (option) {
      chartInstance.value.setOption(option)
    }
    return chartInstance.value
  }

  const updateChart = (option) => {
    if (chartInstance.value) {
      chartInstance.value.setOption(option, { notMerge: true })
    }
  }

  const resizeChart = () => {
    chartInstance.value?.resize()
  }

  const disposeChart = () => {
    if (chartInstance.value) {
      chartInstance.value.dispose()
      chartInstance.value = null
    }
  }

  onUnmounted(() => {
    disposeChart()
  })

  return {
    chartInstance,
    isLoading,
    hasError,
    initChart,
    updateChart,
    resizeChart,
    disposeChart
  }
}
