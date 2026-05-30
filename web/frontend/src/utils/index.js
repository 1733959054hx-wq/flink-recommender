export function debounce(func, wait = 300) {
  let timeout = null
  return function executedFunction(...args) {
    const later = () => {
      clearTimeout(timeout)
      timeout = null
    }
    clearTimeout(timeout)
    timeout = setTimeout(later, wait)
    func.apply(this, args)
  }
}

export function throttle(func, wait = 300) {
  let lastTime = 0
  return function executedFunction(...args) {
    const now = Date.now()
    if (now - lastTime >= wait) {
      lastTime = now
      func.apply(this, args)
    }
  }
}

export function formatNumber(num, decimals = 2) {
  if (num == null) return '0'
  const n = Number(num)
  if (isNaN(n)) return '0'
  return n.toFixed(decimals)
}

export function getPopularityLevel(score) {
  if (score >= 0.7) return { level: '高', class: 'high' }
  if (score >= 0.4) return { level: '中', class: 'medium' }
  return { level: '低', class: 'low' }
}

export function parseTime(timeStr) {
  if (!timeStr) return ''
  return timeStr.split(' ')[1] || timeStr
}
