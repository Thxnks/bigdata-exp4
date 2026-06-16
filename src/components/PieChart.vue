<script setup>
import { onMounted, ref } from "vue";
import * as echarts from "echarts";
import { statMock } from "../mock/stat";

const chartRef = ref(null);

onMounted(() => {
  const chart = echarts.init(chartRef.value);

  const airlines = [...new Set(statMock.map(i => i.airlineCode))];

  const data = airlines.map(a => {
    return {
      name: a,
      value: statMock
        .filter(i => i.airlineCode === a)
        .reduce((sum, i) => sum + i.successCount, 0)
    };
  });

  chart.setOption({
    title: { text: "航司占比" },
    tooltip: { trigger: "item" },
    series: [
      {
        type: "pie",
        data
      }
    ]
  });
});
</script>

<template>
  <div ref="chartRef" style="width: 600px; height: 400px;"></div>
</template>