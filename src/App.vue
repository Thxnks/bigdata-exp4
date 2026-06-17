<script setup>
import { ref, computed, onMounted, onUnmounted } from "vue";
import { getStatList } from "./api/stat";

import LineChart from "./components/LineChart.vue";
import BarChart from "./components/BarChart.vue";
import PieChart from "./components/PieChart.vue";

/* ------------------------
   1️⃣ 筛选条件
-------------------------*/
const selectedAirline = ref("");
const selectedHour = ref("");

/* ------------------------
   2️⃣ 基础维度数据
-------------------------*/
const airlines = [...new Set(tableData.value.map(i => i.airlineCode))];
const hours = [...new Set(tableData.value.map(i => i.statHour))];

/* ------------------------
   3️⃣ 过滤后的数据（核心）
-------------------------*/
const filteredData = computed(() => {
  return tableData.value.filter(item => {
    const matchAirline = selectedAirline.value
      ? item.airlineCode === selectedAirline.value
      : true;

    const matchHour = selectedHour.value
      ? item.statHour === selectedHour.value
      : true;

    return matchAirline && matchHour;
  });
});

/* ------------------------
   4️⃣ KPI统计（跟随筛选）
-------------------------*/
const totalCount = computed(() =>
  filteredData.value.reduce((sum, i) => sum + i.successCount, 0)
);

const tableData = ref([]);

onMounted(async () => {
  const res = await getStatList();
  tableData.value = res.data;
});
</script>

<template>
  <div class="dashboard">

    <!-- 标题 -->
    <h1 class="title">📊 航空统计 Dashboard</h1>

    <!-- KPI -->
    <div class="kpi">
      <div class="card">
        <div class="num">{{ filteredData.length }}</div>
        <div class="label">记录数</div>
      </div>

      <div class="card">
        <div class="num">{{ totalCount }}</div>
        <div class="label">成功总数</div>
      </div>

      <div class="card">
        <div class="num">{{ airlines.length }}</div>
        <div class="label">航司数量</div>
      </div>
    </div>

    <!-- 筛选区 -->
    <div class="filter-box">
      <select v-model="selectedAirline">
        <option value="">全部航司</option>
        <option v-for="a in airlines" :key="a" :value="a">
          {{ a }}
        </option>
      </select>

      <select v-model="selectedHour">
        <option value="">全部时间</option>
        <option v-for="h in hours" :key="h" :value="h">
          {{ h }}
        </option>
      </select>

      <button @click="selectedAirline=''; selectedHour=''">
        重置
      </button>
    </div>

    <!-- 图表 -->
    <div class="charts">

      <!-- ⚠️ 关键：把 filteredData 传进去 -->
      <div class="chart-box">
        <LineChart :data="filteredData" />
      </div>

      <div class="chart-box">
        <BarChart :data="filteredData" />
      </div>

      <div class="chart-box">
        <PieChart :data="filteredData" />
      </div>

    </div>

    <!-- 表格 -->
    <div class="table-box">
      <h2>数据明细</h2>

      <table>
        <thead>
          <tr>
            <th>时间</th>
            <th>航司</th>
            <th>成功数</th>
          </tr>
        </thead>

        <tbody>
          <tr v-for="(item, index) in filteredData" :key="index">
            <td>{{ item.statHour }}</td>
            <td>{{ item.airlineCode }}</td>
            <td>{{ item.successCount }}</td>
          </tr>
        </tbody>

      </table>
    </div>

  </div>
</template>

<style scoped>
.dashboard {
  padding: 20px;
  min-height: 100vh;
  background: linear-gradient(135deg, #0f172a, #1e293b);
  color: white;
}

/* KPI */
.kpi {
  display: flex;
  gap: 20px;
  margin-bottom: 20px;
}

.card {
  flex: 1;
  padding: 20px;
  border-radius: 12px;
  background: rgba(255,255,255,0.08);
  backdrop-filter: blur(10px);
  text-align: center;
}

.num {
  font-size: 24px;
  font-weight: bold;
}

/* 筛选 */
.filter-box {
  display: flex;
  gap: 10px;
  margin-bottom: 20px;
}

select, button {
  padding: 8px;
  border-radius: 6px;
  border: none;
}

/* 图表 */
.charts {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 20px;
}

.chart-box {
  background: rgba(255,255,255,0.08);
  padding: 10px;
  border-radius: 12px;
}

/* 表格 */
.table-box {
  margin-top: 20px;
  background: rgba(255,255,255,0.08);
  padding: 20px;
  border-radius: 12px;
}

table {
  width: 100%;
  border-collapse: collapse;
}

th, td {
  padding: 10px;
  text-align: center;
  border-bottom: 1px solid rgba(255,255,255,0.2);
}
</style>