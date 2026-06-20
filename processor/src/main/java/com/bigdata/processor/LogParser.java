package com.bigdata.processor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

// 日志解析器，负责从原始日志行中提取 success 明细。
public class LogParser implements Serializable {
    // 航空公司代码通常由 2 到 3 位大写字母或数字组成。
    private static final Pattern AIRLINE_CODE_PATTERN = Pattern.compile("^[A-Z0-9]{2,3}$");

    // 将单行原始日志解析为一组航空公司 success 明细。
    public List<AirlineSuccessRecord> parseSuccessRecords(String rawLine) {
        // 空行不参与统计。
        if (rawLine == null || rawLine.trim().isEmpty()) {
            return Collections.emptyList();
        }

        // 保留空字段切分，确保字段下标和原始日志一致。
        String[] fields = rawLine.split(",", -1);
        if (fields.length <= 8) {
            return Collections.emptyList();
        }

        // 只处理 ITARES 查询结果日志。
        String logType = fields[1].trim();
        if (!"ITARES".equals(logType)) {
            return Collections.emptyList();
        }

        // 读取日期、小时和 success 字段。
        String date = fields[2].trim();
        String hour = normalizeHour(fields[3].trim());
        String successText = fields[8].trim();

        // 日期或小时格式异常时跳过该行。
        if (!isValidDate(date) || hour == null) {
            return Collections.emptyList();
        }

        // 生成小时粒度统计字段 stat_hour。
        String statHour = date.substring(0, 4) + "-"
                + date.substring(4, 6) + "-"
                + date.substring(6, 8) + " "
                + hour;

        // 每个 success 项生成一条明细，重复项会重复加入列表。
        List<AirlineSuccessRecord> records = new ArrayList<>();
        String[] items = successText.split(";");
        for (String item : items) {
            String trimmed = item.trim();
            if (trimmed.isEmpty() || !trimmed.contains(":")) {
                continue;
            }

            String[] parts = trimmed.split(":", 2);
            String airlineCode = parts[0].trim().toUpperCase(Locale.ROOT);
            String status = parts[1].trim().toLowerCase(Locale.ROOT);

            // 只保留状态为 success 且航空公司代码格式正常的项。
            if ("success".equals(status) && AIRLINE_CODE_PATTERN.matcher(airlineCode).matches()) {
                records.add(new AirlineSuccessRecord(statHour, airlineCode));
            }
        }

        return records;
    }

    private boolean isValidDate(String date) {
        // 日期字段需要是 yyyyMMdd 格式。
        return date != null && date.matches("\\d{8}");
    }

    private String normalizeHour(String hour) {
        // 小时字段需要是 0 到 23 的数字。
        if (hour == null || !hour.matches("\\d{1,2}")) {
            return null;
        }
        int value = Integer.parseInt(hour);
        if (value < 0 || value > 23) {
            return null;
        }
        // 单位数小时补零，保证输出格式一致。
        return value < 10 ? "0" + value : String.valueOf(value);
    }
}
