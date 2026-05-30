<template>
  <div>
    <!-- 悬浮触发按钮 -->
    <div class="floating-trigger" @click="drawerVisible = true">
      <el-icon :size="24" class="trigger-icon">
        <Lightning />
      </el-icon>
      <span class="trigger-text">控制台</span>
    </div>

    <!-- 侧边抽屉 -->
    <el-drawer
      v-model="drawerVisible"
      title="实时行为注入控制台"
      direction="rtl"
      size="450px"
      :with-header="true"
      class="clean-drawer"
    >
      <div class="drawer-content">
        <!-- 行为注入 -->
        <el-card class="input-section" shadow="hover" style="margin-top: 20px">
          <template #header>
            <div class="card-header">
              <el-icon><Operation /></el-icon>
              <span>手动注入突发行为</span>
            </div>
          </template>

          <el-form :model="behaviorForm" label-width="90px" size="default">
            <el-form-item label="用户ID">
              <el-select
                v-model="behaviorForm.user_id"
                placeholder="选择用户"
                filterable
                style="width: 100%"
                :loading="userLoading"
                @focus="loadUsers"
              >
                <el-option
                  v-for="user in userList"
                  :key="user"
                  :label="user"
                  :value="user"
                />
              </el-select>
            </el-form-item>

            <el-form-item label="商品ID">
              <el-select
                v-model="behaviorForm.item_id"
                placeholder="搜索并选择商品"
                filterable
                remote
                :remote-method="searchItems"
                :loading="itemLoading"
                style="width: 100%"
                @focus="loadAllItems"
                @change="onItemChange"
              >
                <el-option
                  v-for="item in itemList"
                  :key="item.itemId"
                  :label="`商品 ${item.itemId} (类目${item.categoryId})`"
                  :value="item.itemId"
                />
              </el-select>
            </el-form-item>

            <el-form-item label="类目ID">
              <el-input
                :value="selectedCategory"
                placeholder="自动匹配"
                disabled
                style="width: 100%"
              />
              <input type="hidden" v-model="behaviorForm.category_id" />
            </el-form-item>

            <el-form-item label="行为类型">
              <el-select v-model="behaviorForm.behavior_type" style="width: 100%">
                <el-option label="浏览 (pv)" value="pv" />
                <el-option label="加购 (cart)" value="cart" />
                <el-option label="收藏 (fav)" value="fav" />
                <el-option label="购买 (buy)" value="buy" />
              </el-select>
            </el-form-item>

            <el-form-item label="时间戳">
              <el-input
                :value="behaviorForm.timestamp"
                placeholder="自动生成"
                disabled
              />
            </el-form-item>

            <el-form-item>
              <el-button
                type="primary"
                size="default"
                style="width: 100%"
                @click="injectBehavior"
                :loading="isInjecting"
              >
                <el-icon style="margin-right: 5px"><Lightning /></el-icon>
                立即注入
              </el-button>
            </el-form-item>
          </el-form>
        </el-card>

        <!-- 模型重训 -->
        <el-card class="input-section" shadow="hover" style="margin-top: 20px">
          <template #header>
            <div class="card-header">
              <el-icon><Operation /></el-icon>
              <span>模型管理</span>
            </div>
          </template>

          <el-alert
            title="系统每 20秒 自动更新用户特征和商品特征"
            type="info"
            :closable="false"
            show-icon
            style="margin-bottom: 15px"
          />

          <el-alert
            title="应对数据概念漂移，支持一键触发全量模型重训"
            type="warning"
            :closable="false"
            show-icon
            style="margin-bottom: 15px"
          />
          <!-- 修改后：用 flex 容器让两个按钮并排对齐 -->
          <div style="display: flex; gap: 10px;">
            <el-button
                type="warning"
                size="default"
                style="flex: 1"
                @click="retrainModel"
                :loading="isRetraining"
            >
              <el-icon style="margin-right: 5px"><Refresh /></el-icon>
              {{ isRetraining ? '重训中...' : '强制触发全量模型重训' }}
            </el-button>
            <el-button
                type="success"
                size="default"
                style="flex: 1"
                @click="evaluateModel"
                :loading="isEvaluating"
            >
              <el-icon style="margin-right: 5px"><DataAnalysis /></el-icon>
              {{ isEvaluating ? '评测中...' : '触发模型离线评测' }}
            </el-button>
          </div>


          <el-progress
              v-if="isRetraining"
              :percentage="retrainProgress"
              :status="retrainStatus"
              style="margin-top: 15px"
          />
        </el-card>


        <!-- 操作日志 -->
        <el-card class="input-section" shadow="hover" style="margin-top: 20px">
          <template #header>
            <div class="card-header">
              <el-icon><Document /></el-icon>
              <span>发送日志</span>
              <el-button
                text
                type="primary"
                size="small"
                @click="clearLogs"
                style="margin-left: auto"
              >
                清空
              </el-button>
            </div>
          </template>

          <div class="log-container">
            <div
              v-for="(log, index) in logs"
              :key="index"
              class="log-item"
              :class="log.type"
            >
              <span class="log-time">{{ log.time }}</span>
              <span class="log-message">{{ log.message }}</span>
            </div>
            <div v-if="logs.length === 0" class="log-empty">
              暂无操作记录
            </div>
          </div>
        </el-card>
      </div>
    </el-drawer>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, onUnmounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Lightning, Operation, Document, Refresh } from '@element-plus/icons-vue'
import { api } from '@/services/api.js'

const drawerVisible = ref(false)

const behaviorForm = reactive({
  user_id: 1001,
  item_id: null,
  category_id: null,
  behavior_type: 'buy',
  timestamp: Math.floor(Date.now() / 1000)
})

const isInjecting = ref(false)
const logs = ref([])

// 模型重训相关
const isRetraining = ref(false)
const retrainProgress = ref(0)
const retrainStatus = ref('')

// 商品列表相关
const itemList = ref([])
const itemLoading = ref(false)

// 类目列表相关
const categoryList = ref([])

// 用户列表相关
const userList = ref([])
const userLoading = ref(false)

// 选中的类目（用于显示）
const selectedCategory = ref('')

// 加载用户列表
const loadUsers = async () => {
  if (userList.value.length > 0) return

  try {
    userLoading.value = true
    const res = await api.getUserList()
    if (res.code === 200) {
      userList.value = res.data
    }
  } catch (error) {
    console.error('加载用户列表失败:', error)
    ElMessage.error('加载用户列表失败')
  } finally {
    userLoading.value = false
  }
}

const loadAllItems = async () => {
  if (itemList.value.length > 0) return

  try {
    itemLoading.value = true
    const res = await api.getItemList('')
    if (res.code === 200) {
      itemList.value = res.data
    }
  } catch (error) {
    console.error('加载商品列表失败:', error)
  } finally {
    itemLoading.value = false
  }
}

const searchItems = async (query) => {
  if (!query) {
    await loadAllItems()
    return
  }

  try {
    itemLoading.value = true
    const res = await api.getItemList(query)
    if (res.code === 200) {
      itemList.value = res.data
    }
  } catch (error) {
    console.error('搜索商品失败:', error)
  } finally {
    itemLoading.value = false
  }
}

const loadCategories = async () => {
  try {
    const res = await api.getCategoryList()
    if (res.code === 200) {
      categoryList.value = res.data
    }
  } catch (error) {
    console.error('加载类目列表失败:', error)
  }
}

const onItemChange = (itemId) => {
  const item = itemList.value.find(i => i.itemId === itemId)
  if (item) {
    behaviorForm.category_id = item.categoryId
    selectedCategory.value = `类目 ${item.categoryId}`
  } else {
    behaviorForm.category_id = null
    selectedCategory.value = ''
  }
}

onMounted(() => {
  loadCategories()
})

onUnmounted(() => {})

const addLog = (message, type = 'info') => {
  const now = new Date()
  const timeStr = `${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}:${now.getSeconds().toString().padStart(2, '0')}`

  logs.value.unshift({
    time: timeStr,
    message,
    type
  })

  if (logs.value.length > 20) {
    logs.value.pop()
  }
}

const clearLogs = () => {
  logs.value = []
  addLog('日志已清空', 'info')
}



const injectBehavior = async () => {
  if (!behaviorForm.user_id || !behaviorForm.item_id) {
    ElMessage.warning('请选择用户和商品')
    return
  }

  isInjecting.value = true
  behaviorForm.timestamp = Math.floor(Date.now() / 1000)

  try {
    const payload = {
      user_id: behaviorForm.user_id,
      item_id: behaviorForm.item_id,
      category_id: behaviorForm.category_id,
      behavior_type: behaviorForm.behavior_type,
      timestamp: behaviorForm.timestamp
    }

    addLog(`准备发送: user=${payload.user_id}, item=${payload.item_id}, type=${payload.behavior_type}`, 'info')

    await api.insertBehavior(payload)

    addLog(`✅ Socket 发送成功！数据已注入 Flink`, 'success')
    ElMessage.success('行为已注入，观察推荐列表变化...')

    // if (queryForm.userId === behaviorForm.user_id) {
    //   addLog(`⚡ 用户 ${behaviorForm.user_id} 的推荐列表将在 3 秒内更新`, 'warning')
    //   // 如果正在追踪该用户，立即刷新
    //   if (trackingTimer) {
    //     await refreshUserData()
    //   }
    // } 假报错来源

    setTimeout(() => {
      drawerVisible.value = false
    }, 1000)

  } catch (error) {
    addLog(`❌ 发送失败: ${error.message}`, 'error')
    ElMessage.error('注入失败: ' + error.message)
  } finally {
    isInjecting.value = false
  }
}

const retrainModel = async () => {
  if (isRetraining.value) {
    ElMessage.warning('模型重训正在进行中，请稍候...')
    return
  }

  try {
    await ElMessageBox.confirm(
      '确定要触发全量模型重训吗？这将消耗较多系统资源。',
      '警告',
      {
        confirmButtonText: '确定重训',
        cancelButtonText: '取消',
        type: 'warning'
      }
    )

    isRetraining.value = true
    retrainProgress.value = 0
    retrainStatus.value = ''

    addLog('🔄 开始触发模型重训...', 'warning')

    const res = await api.retrainModel()

    if (res.code === 200) {
      addLog('✅ 模型重训任务已启动', 'success')
      ElMessage.success('模型重训任务已启动，请查看后台日志')

      let progress = 0
      const timer = setInterval(() => {
        progress += 10
        retrainProgress.value = progress
        if (progress >= 100) {
          clearInterval(timer)
          retrainStatus.value = 'success'
          isRetraining.value = false
          addLog('✅ 模型重训完成', 'success')
          ElMessage.success('模型重训完成！推荐结果已更新')
        }
      }, 1000)
    } else {
      throw new Error(res.message)
    }

  } catch (error) {
    if (error !== 'cancel') {
      addLog(`❌ 模型重训失败: ${error.message}`, 'error')
      ElMessage.error('模型重训失败: ' + error.message)
      isRetraining.value = false
      retrainStatus.value = 'exception'
    }
  }
}
const isEvaluating = ref(false)

const evaluateModel = async () => {
  if (isEvaluating.value) {
    ElMessage.warning('模型评测正在进行中，请稍候...')
    return
  }

  try {
    await ElMessageBox.confirm(
        '确定要触发模型离线评测吗？评测结果将写入数据库。',
        '确认',
        {
          confirmButtonText: '确定评测',
          cancelButtonText: '取消',
          type: 'info'
        }
    )

    isEvaluating.value = true
    addLog('🔄 开始触发模型评测...', 'warning')

    const res = await api.evaluateModel()

    if (res.code === 200) {
      addLog('✅ 模型评测任务已启动', 'success')
      ElMessage.success('模型评测任务已启动，完成后可通过 PR-AUC 接口查看结果')
    } else {
      throw new Error(res.message)
    }

  } catch (error) {
    if (error !== 'cancel') {
      addLog(`❌ 模型评测失败: ${error.message}`, 'error')
      ElMessage.error('模型评测失败: ' + error.message)
    }
  } finally {
    isEvaluating.value = false
  }
}

</script>

<style scoped>
.floating-trigger {
  position: fixed;
  right: 30px;
  bottom: 30px;
  width: 60px;
  height: 60px;
  background: #409eff;
  border-radius: 50%;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  box-shadow: 0 2px 12px rgba(64, 158, 255, 0.3);
  transition: all 0.3s ease;
  z-index: 9999;
}

.floating-trigger:hover {
  transform: scale(1.1);
  box-shadow: 0 4px 16px rgba(64, 158, 255, 0.5);
  background: #66b1ff;
}

.trigger-icon {
  color: white;
  margin-bottom: 2px;
}

.trigger-text {
  font-size: 10px;
  color: white;
  font-weight: bold;
}
</style>

<style>
/* 悬浮触发按钮 - 全局样式 */
.floating-trigger {
  position: fixed !important;
  right: 30px !important;
  bottom: 30px !important;
  width: 60px !important;
  height: 60px !important;
  background: #409eff !important;
  border-radius: 50% !important;
  display: flex !important;
  flex-direction: column !important;
  align-items: center !important;
  justify-content: center !important;
  cursor: pointer !important;
  box-shadow: 0 2px 12px rgba(64, 158, 255, 0.3) !important;
  transition: all 0.3s ease !important;
  z-index: 9999 !important;
}

.floating-trigger:hover {
  transform: scale(1.1) !important;
  box-shadow: 0 4px 16px rgba(64, 158, 255, 0.5) !important;
  background: #66b1ff !important;
}

.trigger-icon {
  color: white !important;
  margin-bottom: 2px !important;
}

.trigger-text {
  font-size: 10px !important;
  color: white !important;
  font-weight: bold !important;
}

/* 全局样式 - 简洁白色主题 */
.clean-drawer .el-drawer {
  background: #ffffff !important;
}

.clean-drawer .el-drawer__header {
  background: #f5f7fa !important;
  color: #303133 !important;
  border-bottom: 1px solid #e4e7ed !important;
  margin-bottom: 0 !important;
  padding: 18px 20px !important;
  font-size: 16px !important;
  font-weight: 600 !important;
}

.clean-drawer .el-drawer__title {
  color: #303133 !important;
  font-size: 16px !important;
  font-weight: 600 !important;
}

.clean-drawer .el-drawer__close-btn {
  color: #909399 !important;
}

.clean-drawer .el-drawer__close-btn:hover {
  color: #409eff !important;
}

.clean-drawer .el-drawer__body {
  padding: 20px !important;
  background: #f5f7fa !important;
}

.drawer-content {
  padding: 0;
}

.input-section {
  background: #ffffff !important;
  border: 1px solid #e4e7ed !important;
  border-radius: 4px !important;
}

.input-section .el-card__header {
  background: #fafafa !important;
  border-bottom: 1px solid #ebeef5 !important;
  padding: 12px 20px !important;
}

.card-header {
  display: flex;
  align-items: center;
  gap: 8px;
  color: #303133 !important;
  font-weight: 600 !important;
  font-size: 14px !important;
}

.input-section .el-form-item__label {
  color: #606266 !important;
  font-size: 14px !important;
}

.input-section .el-input__wrapper {
  background: #ffffff !important;
  box-shadow: 0 0 0 1px #dcdfe6 inset !important;
  border-radius: 4px !important;
}

.input-section .el-input__inner {
  color: #606266 !important;
  font-size: 14px !important;
}

.input-section .el-input__inner::placeholder {
  color: #c0c4cc !important;
}

.input-section .el-select .el-input__wrapper {
  background: #ffffff !important;
}

.input-section .el-select-dropdown {
  background: #ffffff !important;
  border: 1px solid #e4e7ed !important;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.1) !important;
}

.input-section .el-select-dropdown__item {
  color: #606266 !important;
}

.input-section .el-select-dropdown__item.hover,
.input-section .el-select-dropdown__item:hover {
  background: #ecf5ff !important;
  color: #409eff !important;
}

.input-section .el-button--primary {
  background: #409eff !important;
  border-color: #409eff !important;
  color: #ffffff !important;
}

.input-section .el-button--primary:hover {
  background: #66b1ff !important;
  border-color: #66b1ff !important;
}

.input-section .el-button--warning {
  background: #e6a23c !important;
  border-color: #e6a23c !important;
  color: #ffffff !important;
}

.input-section .el-button--warning:hover {
  background: #ebb563 !important;
  border-color: #ebb563 !important;
}

.log-container {
  max-height: 200px;
  overflow-y: auto;
  font-family: 'Courier New', monospace;
  font-size: 12px;
  background: #f5f7fa !important;
  border: 1px solid #e4e7ed !important;
  border-radius: 4px !important;
  padding: 12px !important;
}

.log-item {
  padding: 8px 10px;
  margin-bottom: 6px;
  border-radius: 3px;
  display: flex;
  gap: 10px;
  font-size: 12px;
}

.log-item.success {
  background: #f0f9eb !important;
  border-left: 3px solid #67c23a !important;
  color: #67c23a !important;
}

.log-item.error {
  background: #fef0f0 !important;
  border-left: 3px solid #f56c6c !important;
  color: #f56c6c !important;
}

.log-item.warning {
  background: #fdf6ec !important;
  border-left: 3px solid #e6a23c !important;
  color: #e6a23c !important;
}

.log-item.info {
  background: #ecf5ff !important;
  border-left: 3px solid #409eff !important;
  color: #409eff !important;
}

.log-time {
  color: #909399 !important;
  min-width: 60px;
  font-size: 11px;
}

.log-message {
  color: #606266 !important;
  flex: 1;
}

.log-empty {
  text-align: center;
  color: #c0c4cc !important;
  padding: 30px;
  font-size: 13px;
}

.log-container::-webkit-scrollbar {
  width: 6px;
}

.log-container::-webkit-scrollbar-track {
  background: #f5f7fa;
}

.log-container::-webkit-scrollbar-thumb {
  background: #dcdfe6;
  border-radius: 3px;
}

.log-container::-webkit-scrollbar-thumb:hover {
  background: #c0c4cc;
}
</style>
