"use client";

import { useQuery } from "@tanstack/react-query";
import { useDeferredDesktopActivation } from "@/hooks/useDeferredDesktopActivation";
import { useDesktopPageActive } from "@/hooks/useDesktopPageActive";
import { dashboardClient } from "@/lib/api/dashboard-client";
import { useAppStore } from "@/lib/store/useAppStore";
import type { DashboardAdminUsageSummary } from "@/types";

export const DASHBOARD_ADMIN_USAGE_QUERY_KEY = [
  "dashboard",
  "admin-usage-summary",
] as const;

interface DashboardAdminUsageSummaryQueryParams {
  startTs?: number | null;
  endTs?: number | null;
}

export function useDashboardAdminUsageSummary(
  params?: DashboardAdminUsageSummaryQueryParams,
  enabled = true,
) {
  const serviceStatus = useAppStore((state) => state.serviceStatus);
  const isPageActive = useDesktopPageActive("/");
  const isServiceReady = serviceStatus.connected;
  const isQueryEnabled = useDeferredDesktopActivation(
    enabled && isServiceReady && isPageActive,
  );

  const query = useQuery<DashboardAdminUsageSummary>({
    queryKey: [
      ...DASHBOARD_ADMIN_USAGE_QUERY_KEY,
      serviceStatus.addr,
      params?.startTs ?? null,
      params?.endTs ?? null,
    ],
    queryFn: () =>
      dashboardClient.getAdminUsageSummary({
        startTs: params?.startTs ?? null,
        endTs: params?.endTs ?? null,
      }),
    enabled: isQueryEnabled,
    retry: 1,
    staleTime: 30_000,
  });

  return {
    ...query,
    isServiceReady,
  };
}
