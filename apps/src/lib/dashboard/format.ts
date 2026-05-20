"use client";

import { formatCompactNumber } from "@/lib/utils/usage";

export function formatPercent(value: number | null | undefined): string {
  return value == null ? "--" : `${Math.max(0, Math.round(value))}%`;
}

export function formatCompactTokenAmount(value: number | null | undefined): string {
  const normalized =
    typeof value === "number" && Number.isFinite(value) ? Math.max(0, value) : 0;
  if (normalized < 1000) {
    return normalized.toLocaleString("zh-CN", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }
  return formatCompactNumber(normalized, "0.00", 2, true);
}

export function estimateChartYAxisWidth(
  values: Array<number | null | undefined>,
  formatter: (value: number) => string,
  minimumWidth = 44,
): number {
  const widestLabelLength = values.reduce<number>((maxLength, value) => {
    const normalizedValue = typeof value === "number" && Number.isFinite(value) ? value : 0;
    const normalized = Math.max(0, normalizedValue);
    return Math.max(maxLength, formatter(normalized).length);
  }, 0);

  return Math.max(minimumWidth, Math.ceil(widestLabelLength * 8 + 16));
}
