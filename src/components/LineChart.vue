<script setup>
import { onMounted, ref } from "vue";
import * as echarts from "echarts";
import { statMock } from "../mock/stat";

const chartRef = ref(null);

onMounted(() => {
  const chart = echarts.init(chartRef.value);

  // 处理数据：按时间汇总 successCount
  const hours = [...new Set(statMock.map(i => i.statHour))];

  const sumByHour = hours.map(h => {
    return statMock
      .filter(i => i.statHour === h)
      .reduce((sum, i) => sum + i.successCount, 0);
  });

  const option = {
    title: { text: "成功数趋势图" },
    tooltip: {},
    xAxis: {
      type: "category",
      data: hours
    },
    yAxis: {
      type: "value"
    },
    series: [
      {
        type: "line",
        data: sumByHour
      }
    ]
  };

  chart.setOption(option);
});
</script>

<template>
  <div ref="chartRef" style="width: 600px; height: 400px;"></div>
</template>