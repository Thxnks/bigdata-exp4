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

  const airlines = [...new Set(props.data.map(i => i.airlineCode))];

  const values = airlines.map(a => {
    return props.data
      .filter(i => i.airlineCode === a)
      .reduce((sum, i) => sum + i.successCount, 0);
  });

  chart.setOption({
    title: { text: "各航司成功数" },
    tooltip: {},
    xAxis: { type: "category", data: airlines },
    yAxis: { type: "value" },
    series: [
      {
        type: "bar",
        data: values
      }
    ]
  });
}

onMounted(() => {
  chart = echarts.init(document.getElementById("barChart"));
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
  <div id="barChart" style="height: 300px;"></div>
</template>