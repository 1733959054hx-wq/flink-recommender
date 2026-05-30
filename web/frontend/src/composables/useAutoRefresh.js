import { ref, onMounted, onUnmounted } from 'vue'

export function useAutoRefresh(fetchFn, interval = 3000, immediate = true) {
  const data = ref(null)
  const isLoading = ref(false)
  const error = ref(null)
  let timer = null

  const fetch = async () => {
    isLoading.value = true
    error.value = null
    try {
      data.value = await fetchFn()
    } catch (e) {
      error.value = e
    } finally {
      isLoading.value = false
    }
  }

  const start = () => {
    stop()
    if (immediate) {
      fetch()
    }
    timer = setInterval(fetch, interval)
  }

  const stop = () => {
    if (timer) {
      clearInterval(timer)
      timer = null
    }
  }

  const refresh = () => {
    fetch()
  }

  onMounted(() => {
    start()
  })

  onUnmounted(() => {
    stop()
  })

  return {
    data,
    isLoading,
    error,
    refresh,
    start,
    stop
  }
}
