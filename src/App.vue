<script setup>
import { ref } from "vue";
import { statMock } from "./mock/stat";

import LineChart from "./components/LineChart.vue";
import BarChart from "./components/BarChart.vue";
import PieChart from "./components/PieChart.vue";

const list = ref(statMock);
</script>

<template>
  <div class="dashboard">

    <!-- 标题 -->
    <h1 class="title">📊 航空统计 Dashboard</h1>

    <!-- KPI卡片 -->
    <div class="kpi">
      <div class="card">
        <div class="num">{{ list.length }}</div>
        <div class="label">数据记录</div>
      </div>

      <div class="card">
        <div class="num">
          {{ list.reduce((s, i) => s + i.successCount, 0) }}
        </div>
        <div class="label">成功总数</div>
      </div>

      <div class="card">
        <div class="num">
          {{ new Set(list.map(i => i.airlineCode)).size }}
        </div>
        <div class="label">航司数量</div>
      </div>
    </div>

    <!-- 图表区域 -->
    <div class="charts">

      <div class="chart-box">
        <LineChart />
      </div>

      <div class="chart-box">
        <BarChart />
      </div>

      <div class="chart-box">
        <PieChart />
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
          <tr v-for="(item, index) in list" :key="index">
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
  background: #f5f7fb;
  min-height: 100vh;
}

.title {
  margin-bottom: 20px;
}

/* KPI */
.kpi {
  display: flex;
  gap: 20px;
  margin-bottom: 20px;
}

.card {
  flex: 1;
  background: white;
  padding: 20px;
  border-radius: 12px;
  text-align: center;
  box-shadow: 0 2px 8px rgba(0,0,0,0.05);
}

.num {
  font-size: 24px;
  font-weight: bold;
}

.label {
  color: #666;
}

/* charts */
.charts {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 20px;
}

.chart-box {
  background: white;
  padding: 10px;
  border-radius: 12px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.05);
}

/* table */
.table-box {
  margin-top: 20px;
  background: white;
  padding: 20px;
  border-radius: 12px;
}

table {
  width: 100%;
  border-collapse: collapse;
}

th, td {
  padding: 10px;
  border-bottom: 1px solid #eee;
  text-align: center;
}
</style>