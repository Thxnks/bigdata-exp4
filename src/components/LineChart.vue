<script setup>
import { onMounted, watch } from "vue";
import * as echarts from "echarts";

const props = defineProps({
  data: {
    type: Array,
    default: () => []
  }
});

let chart = null;

function renderChart() {
  if (!chart) return;

  const hours = [...new Set(props.data.map(i => i.statHour))];

  const seriesData = hours.map(h => {
    return props.data
      .filter(i => i.statHour === h)
      .reduce((sum, i) => sum + i.successCount, 0);
  });

  chart.setOption({
    title: { text: "成功数趋势" },
    tooltip: {},
    xAxis: { type: "category", data: hours },
    yAxis: { type: "value" },
    series: [
      {
        type: "line",
        data: seriesData,
        smooth: true
      }
    ]
  });
}

onMounted(() => {
  chart = echarts.init(document.getElementById("lineChart"));
  renderChart();
});

watch(
  () => props.data,
  () => {
    renderChart();
  },
  { deep: true }
);
</script>

<template>
  <div id="lineChart" style="height: 300px;"></div>
</template>