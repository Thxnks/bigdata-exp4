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

  const pieData = airlines.map(a => ({
    name: a,
    value: props.data
      .filter(i => i.airlineCode === a)
      .reduce((sum, i) => sum + i.successCount, 0)
  }));

  chart.setOption({
    title: { text: "航司占比" },
    tooltip: { trigger: "item" },
    series: [
      {
        type: "pie",
        radius: "60%",
        data: pieData
      }
    ]
  });
}

onMounted(() => {
  chart = echarts.init(document.getElementById("pieChart"));
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
  <div id="pieChart" style="height: 300px;"></div>
</template>