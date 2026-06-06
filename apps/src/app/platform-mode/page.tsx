"use client";

import {
  useMemo,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  AlertTriangle,
  ChevronDown,
  KeyRound,
  Loader2,
  Network,
  RefreshCw,
  RotateCcw,
  Save,
  ShieldCheck,
  TerminalSquare,
  Trash2,
  UserRoundCheck,
  Wrench,
} from "lucide-react";
import { toast } from "sonner";
import {
  CODEX_PROFILE_CANDIDATES_QUERY_KEY,
  CODEX_PROFILE_STATUS_QUERY_KEY,
  codexProfileClient,
} from "@/lib/api/codex-profile-client";
import { getAppErrorMessage } from "@/lib/api/transport";
import {
  buildOpenAiGatewayEndpoint,
  resolveGatewayOrigin,
} from "@/lib/gateway/endpoints";
import {
  CODEX_PROFILE_MODE_LABELS,
  useCodexProfileModeStatus,
} from "@/hooks/useCodexProfileModeStatus";
import { useRuntimeCapabilities } from "@/hooks/useRuntimeCapabilities";
import { useDesktopPageActive } from "@/hooks/useDesktopPageActive";
import { useAppStore } from "@/lib/store/useAppStore";
import { useI18n } from "@/lib/i18n/provider";
import { buildStaticRouteUrl } from "@/lib/utils/static-routes";
import { cn } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Alert,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import type {
  CodexProfileAccountCandidate,
  CodexProfileApiKeyCandidate,
  CodexProfileHistoryRepairSummary,
  CodexProfileMode,
} from "@/types";

const EMPTY_CANDIDATES = { accounts: [], apiKeys: [] };

function formatTime(ts: number | null): string {
  if (!ts) return "-";
  return new Date(ts * 1000).toLocaleString();
}

function formatBytes(bytes: number | null | undefined): string {
  const value = typeof bytes === "number" && Number.isFinite(bytes) ? bytes : 0;
  if (value < 1024) return `${value} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let size = value / 1024;
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index += 1;
  }
  return `${size.toFixed(size >= 10 ? 1 : 2)} ${units[index]}`;
}

function keyLabel(key: CodexProfileApiKeyCandidate): string {
  return key.name || key.modelSlug || key.id;
}

function accountLabel(account: CodexProfileAccountCandidate): string {
  return account.groupName ? `${account.label} · ${account.groupName}` : account.label;
}

function historyRepairChangeCount(
  summary: CodexProfileHistoryRepairSummary | null,
): number {
  if (!summary) return 0;
  return (
    summary.changedRolloutFileCount +
    summary.updatedSqliteRowCount +
    summary.addedSessionIndexEntryCount
  );
}

function pickAvailableCandidateId<T extends { id: string }>(
  preferredId: string | null | undefined,
  managedId: string | null | undefined,
  candidates: T[],
): string {
  const ids = new Set(candidates.map((item) => item.id));
  if (preferredId && ids.has(preferredId)) return preferredId;
  if (managedId && ids.has(managedId)) return managedId;
  return candidates[0]?.id || "";
}

function modeImpact(mode: CodexProfileMode | null): string {
  if (mode === "direct_account") {
    return "当前为账号直连，Codex CLI 直连 OpenAI，CodexManager 无法统计 CLI 请求日志和用量。";
  }
  if (mode === "gateway") {
    return "当前为本地网关，Codex CLI 经过 CodexManager 转发，请求日志、Token 和费用统计可用。";
  }
  return "选择账号直连或本地网关后，CodexManager 会接管该 Codex profile 的 auth.json / config.toml。";
}

function ModeFact({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl border border-border/60 bg-background/35 p-3">
      <p className="text-[11px] text-muted-foreground">{label}</p>
      <p className="mt-1 truncate text-sm font-semibold">{value || "-"}</p>
    </div>
  );
}

function ActionLink({
  href,
  children,
}: {
  href: string;
  children: ReactNode;
}) {
  return (
    <a
      href={buildStaticRouteUrl(href)}
      className="inline-flex h-8 w-fit items-center justify-center rounded-lg border border-border bg-background px-3 text-sm font-medium text-foreground transition-colors hover:bg-muted"
    >
      {children}
    </a>
  );
}

export default function PlatformModePage() {
  const { t } = useI18n();
  const queryClient = useQueryClient();
  const serviceStatus = useAppStore((state) => state.serviceStatus);
  const { mode, canAccessManagementRpc } = useRuntimeCapabilities();
  const isServiceReady = canAccessManagementRpc && serviceStatus.connected;
  const isPageActive = useDesktopPageActive("/platform-mode/");
  const [codexHomeDraft, setCodexHomeDraft] = useState<string | null>(null);
  const [selectedAccountIdDraft, setSelectedAccountIdDraft] = useState<string | null>(
    null,
  );
  const [selectedApiKeyIdDraft, setSelectedApiKeyIdDraft] = useState<string | null>(
    null,
  );
  const [gatewayBaseUrlDraft, setGatewayBaseUrlDraft] = useState<string | null>(
    null,
  );
  const browserOrigin = useSyncExternalStore(
    () => () => undefined,
    () =>
      mode === "web-gateway" && typeof window !== "undefined"
        ? window.location.origin
        : "",
    () => "",
  );

  const defaultGatewayBaseUrl = useMemo(() => {
    const origin = resolveGatewayOrigin({
      browserOrigin,
      runtimeMode: mode,
      serviceAddr: serviceStatus.addr,
    });
    return buildOpenAiGatewayEndpoint(origin);
  }, [browserOrigin, mode, serviceStatus.addr]);

  const statusQuery = useCodexProfileModeStatus();
  const candidatesQuery = useQuery({
    queryKey: CODEX_PROFILE_CANDIDATES_QUERY_KEY,
    queryFn: () => codexProfileClient.listCandidates(),
    enabled: isServiceReady,
    retry: 1,
    staleTime: 0,
    refetchInterval: isServiceReady && isPageActive ? 5_000 : false,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: true,
  });

  const status = statusQuery.status;
  const candidates = candidatesQuery.data || EMPTY_CANDIDATES;
  const codexHomeInput = codexHomeDraft ?? status?.codexHome ?? "";
  const selectedAccountId = pickAvailableCandidateId(
    selectedAccountIdDraft,
    status?.selectedAccountId,
    candidates.accounts,
  );
  const selectedApiKeyId = pickAvailableCandidateId(
    selectedApiKeyIdDraft,
    status?.selectedApiKeyId,
    candidates.apiKeys,
  );
  const gatewayBaseUrl =
    gatewayBaseUrlDraft ?? status?.gatewayBaseUrl ?? defaultGatewayBaseUrl;
  const isDirectActive = status?.mode === "direct_account";
  const isGatewayActive = status?.mode === "gateway";
  const activeAccountValue = status?.selectedAccountId
    ? candidates.accounts.find((item) => item.id === status.selectedAccountId)?.label ||
      status.selectedAccountId
    : "-";
  const activeKeyValue = status?.selectedApiKeyId
    ? candidates.apiKeys.find((item) => item.id === status.selectedApiKeyId)?.name ||
      status.selectedApiKeyId
    : "-";

  const refreshAll = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: CODEX_PROFILE_STATUS_QUERY_KEY }),
      queryClient.invalidateQueries({ queryKey: CODEX_PROFILE_CANDIDATES_QUERY_KEY }),
    ]);
  };

  const showHistoryRepairToast = (
    summary: CodexProfileHistoryRepairSummary | null,
  ) => {
    if (!summary) return;
    if (summary.warnings.length > 0) {
      toast.warning(`${t("历史修复完成但有警告")}：${summary.warnings[0]}`);
      return;
    }
    if (historyRepairChangeCount(summary) > 0) {
      toast.success(t("历史会话可见性已修复"));
    }
  };

  const saveConfigMutation = useMutation({
    mutationFn: () => codexProfileClient.setConfig(codexHomeInput),
    onSuccess: async (nextStatus) => {
      setCodexHomeDraft(nextStatus.codexHome);
      await refreshAll();
      toast.success(t("Codex profile 路径已保存"));
    },
    onError: (error: unknown) => {
      toast.error(`${t("保存失败")}: ${getAppErrorMessage(error)}`);
    },
  });

  const applyDirectMutation = useMutation({
    mutationFn: () =>
      codexProfileClient.applyDirectAccount({
        accountId: selectedAccountId,
        codexHome: codexHomeInput,
    }),
    onSuccess: async (nextStatus) => {
      await refreshAll();
      toast.success(t("已切换到账号直连"));
      showHistoryRepairToast(nextStatus.historyRepair);
    },
    onError: (error: unknown) => {
      toast.error(`${t("切换失败")}: ${getAppErrorMessage(error)}`);
    },
  });

  const applyGatewayMutation = useMutation({
    mutationFn: () =>
      codexProfileClient.applyGateway({
        apiKeyId: selectedApiKeyId,
        codexHome: codexHomeInput,
        baseUrl: gatewayBaseUrl,
    }),
    onSuccess: async (nextStatus) => {
      await refreshAll();
      toast.success(t("已切换到本地网关"));
      showHistoryRepairToast(nextStatus.historyRepair);
    },
    onError: (error: unknown) => {
      toast.error(`${t("切换失败")}: ${getAppErrorMessage(error)}`);
    },
  });

  const restoreMutation = useMutation({
    mutationFn: () => codexProfileClient.restore(codexHomeInput),
    onSuccess: async () => {
      await refreshAll();
      toast.success(t("已恢复接管前的 Codex 配置"));
    },
    onError: (error: unknown) => {
      toast.error(`${t("恢复失败")}: ${getAppErrorMessage(error)}`);
    },
  });

  const repairHistoryMutation = useMutation({
    mutationFn: () => codexProfileClient.repairHistory(codexHomeInput),
    onSuccess: async (summary) => {
      await refreshAll();
      showHistoryRepairToast(summary);
      if (summary.warnings.length === 0 && historyRepairChangeCount(summary) === 0) {
        toast.success(t("历史会话已与当前模式一致"));
      }
    },
    onError: (error: unknown) => {
      toast.error(`${t("修复失败")}: ${getAppErrorMessage(error)}`);
    },
  });

  const pruneHistoryBackupsMutation = useMutation({
    mutationFn: () => codexProfileClient.pruneHistoryBackups(codexHomeInput),
    onSuccess: async (result) => {
      await refreshAll();
      if (result.warnings.length > 0) {
        toast.warning(`${t("清理完成但有警告")}：${result.warnings[0]}`);
        return;
      }
      toast.success(
        t("已清理 {count} 份历史备份，释放 {bytes}", {
          count: result.removedCount,
          bytes: formatBytes(result.removedBytes),
        }),
      );
    },
    onError: (error: unknown) => {
      toast.error(`${t("清理失败")}: ${getAppErrorMessage(error)}`);
    },
  });

  const isMutating =
    saveConfigMutation.isPending ||
    applyDirectMutation.isPending ||
    applyGatewayMutation.isPending ||
    restoreMutation.isPending ||
    repairHistoryMutation.isPending ||
    pruneHistoryBackupsMutation.isPending;
  const latestHistoryRepair =
    repairHistoryMutation.data ||
    applyDirectMutation.data?.historyRepair ||
    applyGatewayMutation.data?.historyRepair ||
    status?.historyRepair ||
    null;

  return (
    <main className="flex w-full flex-col gap-5 px-4 py-4 md:px-6">
      <div className="flex flex-col gap-2">
        <div className="flex flex-wrap items-center gap-3">
          <div className="flex size-10 items-center justify-center rounded-xl bg-primary/10 text-primary">
            <TerminalSquare className="size-5" />
          </div>
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">
              {t("平台模式选择")}
            </h1>
            <p className="text-sm text-muted-foreground">
              {t("选择 Codex CLI 直连账号，或通过 CodexManager 本地网关接入。")}
            </p>
          </div>
        </div>
      </div>

      <Alert className="border-amber-500/30 bg-amber-500/10">
        <AlertTriangle className="size-4" />
        <AlertTitle>{t("写入位置说明")}</AlertTitle>
        <AlertDescription>
          {t(
            "这里修改的是 codexmanager-service 所在机器的 Codex 配置目录，不一定是当前浏览器所在机器。",
          )}
        </AlertDescription>
      </Alert>

      {!isServiceReady ? (
        <Alert variant="destructive">
          <AlertTriangle className="size-4" />
          <AlertTitle>{t("服务未连接")}</AlertTitle>
          <AlertDescription>
            {t("当前运行环境无法访问管理 RPC，暂时不能读取或写入 Codex profile。")}
          </AlertDescription>
        </Alert>
      ) : null}

      {status?.warnings.length ? (
        <Alert className="border-amber-500/30 bg-amber-500/10">
          <AlertTriangle className="size-4" />
          <AlertTitle>{t("Profile 迁移警告")}</AlertTitle>
          <AlertDescription>{status.warnings[0]}</AlertDescription>
        </Alert>
      ) : null}

      <div className="grid gap-5 lg:grid-cols-2 xl:grid-cols-[minmax(320px,0.9fr)_minmax(0,1.05fr)_minmax(0,1.05fr)]">
        <Card className="overflow-hidden border-primary/20 bg-primary/5 shadow-sm lg:col-span-2 xl:col-span-1">
          <CardHeader className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between xl:flex-col 2xl:flex-row">
            <div>
              <CardTitle className="flex flex-wrap items-center gap-2 text-xl">
                {t("当前模式")}
                <Badge variant={isGatewayActive ? "default" : "secondary"}>
                  {status ? t(CODEX_PROFILE_MODE_LABELS[status.mode]) : "-"}
                </Badge>
              </CardTitle>
              <CardDescription className="mt-2 text-sm">
                {t(modeImpact(status?.mode ?? null))}
              </CardDescription>
            </div>
            <Button
              type="button"
              variant="outline"
              onClick={() => refreshAll()}
              disabled={!isServiceReady || statusQuery.isFetching}
              className="w-fit"
            >
              <RefreshCw
                className={
                  statusQuery.isFetching || candidatesQuery.isFetching
                    ? "size-4 animate-spin"
                    : "size-4"
                }
              />
              {t("刷新状态")}
            </Button>
          </CardHeader>
          <CardContent className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4 xl:grid-cols-1 2xl:grid-cols-2">
            <ModeFact label={t("Codex profile")} value={status?.codexHome || "-"} />
            <ModeFact label={t("当前账号")} value={activeAccountValue} />
            <ModeFact label={t("当前平台 Key")} value={activeKeyValue} />
            <ModeFact
              label={t("最后应用")}
              value={formatTime(status?.lastAppliedAt ?? null)}
            />
          </CardContent>
        </Card>

        <Card
          className={cn(
            "h-full border-border/70 transition-colors",
            isDirectActive && "border-primary/50 bg-primary/5",
          )}
        >
          <CardHeader>
            <div className="flex flex-wrap items-center gap-2">
              <UserRoundCheck className="size-4 text-primary" />
              <CardTitle>{t("账号直连")}</CardTitle>
              {isDirectActive ? <Badge>{t("正在使用")}</Badge> : null}
            </div>
            <CardDescription>
              {t(
                "直连 OpenAI 官方后端，不经过 CodexManager 网关；不会产生 CodexManager 请求日志，仪表盘用量统计不可用。",
              )}
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-4">
            {candidates.accounts.length === 0 && !candidatesQuery.isLoading ? (
              <div className="grid gap-3 rounded-xl border border-dashed border-border/70 bg-muted/25 p-4 text-sm text-muted-foreground">
                <p>{t("没有可用于账号直连的 active OpenAI 账号。")}</p>
                <ActionLink href="/accounts">{t("去添加 OpenAI 账号")}</ActionLink>
              </div>
            ) : (
              <div className="grid gap-2">
                <Label>{t("OpenAI 账号")}</Label>
                <Select
                  value={selectedAccountId}
                  onValueChange={(value) =>
                    setSelectedAccountIdDraft(String(value || ""))
                  }
                  disabled={!isServiceReady || isMutating || candidates.accounts.length === 0}
                >
                  <SelectTrigger className="w-full">
                    <SelectValue placeholder={t("选择账号")}>
                      {(value) =>
                        candidates.accounts.find((item) => item.id === value)?.label ||
                        t("选择账号")
                      }
                    </SelectValue>
                  </SelectTrigger>
                  <SelectContent align="start">
                    <SelectGroup>
                      {candidates.accounts.map((account) => (
                        <SelectItem key={account.id} value={account.id}>
                          {accountLabel(account)}
                        </SelectItem>
                      ))}
                    </SelectGroup>
                  </SelectContent>
                </Select>
                <p className="text-xs text-muted-foreground">
                  {candidatesQuery.isLoading
                    ? t("正在读取可用账号...")
                    : t("可用账号数：{count}", { count: candidates.accounts.length })}
                </p>
              </div>
            )}
            <Button
              type="button"
              onClick={() => applyDirectMutation.mutate()}
              disabled={!isServiceReady || isMutating || !selectedAccountId}
              className="w-fit"
            >
              {applyDirectMutation.isPending ? (
                <Loader2 className="size-4 animate-spin" />
              ) : (
                <ShieldCheck className="size-4" />
              )}
              {isDirectActive ? t("重新应用账号直连") : t("切换到账号直连")}
            </Button>
          </CardContent>
        </Card>

        <Card
          className={cn(
            "h-full border-border/70 transition-colors",
            isGatewayActive && "border-primary/50 bg-primary/5",
          )}
        >
          <CardHeader>
            <div className="flex flex-wrap items-center gap-2">
              <Network className="size-4 text-primary" />
              <CardTitle>{t("本地网关")}</CardTitle>
              {isGatewayActive ? <Badge>{t("正在使用")}</Badge> : null}
            </div>
            <CardDescription>
              {t(
                "通过 CodexManager 本地网关转发 Codex CLI 请求；请求日志、Token、费用估算和仪表盘统计可用。",
              )}
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-4">
            {candidates.apiKeys.length === 0 && !candidatesQuery.isLoading ? (
              <div className="grid gap-3 rounded-xl border border-dashed border-border/70 bg-muted/25 p-4 text-sm text-muted-foreground">
                <p>{t("没有可用于本地网关的平台密钥。")}</p>
                <ActionLink href="/apikeys">{t("去创建平台密钥")}</ActionLink>
              </div>
            ) : (
              <div className="grid gap-2">
                <Label>{t("平台密钥")}</Label>
                <Select
                  value={selectedApiKeyId}
                  onValueChange={(value) =>
                    setSelectedApiKeyIdDraft(String(value || ""))
                  }
                  disabled={!isServiceReady || isMutating || candidates.apiKeys.length === 0}
                >
                  <SelectTrigger className="w-full">
                    <SelectValue placeholder={t("选择平台密钥")}>
                      {(value) => {
                        const key = candidates.apiKeys.find((item) => item.id === value);
                        return key ? keyLabel(key) : t("选择平台密钥");
                      }}
                    </SelectValue>
                  </SelectTrigger>
                  <SelectContent align="start">
                    <SelectGroup>
                      {candidates.apiKeys.map((key) => (
                        <SelectItem key={key.id} value={key.id}>
                          {keyLabel(key)}
                        </SelectItem>
                      ))}
                    </SelectGroup>
                  </SelectContent>
                </Select>
                <p className="text-xs text-muted-foreground">
                  {t("将使用 gateway base_url")}：{gatewayBaseUrl || "-"}
                </p>
              </div>
            )}
            <Button
              type="button"
              onClick={() => applyGatewayMutation.mutate()}
              disabled={
                !isServiceReady ||
                isMutating ||
                !selectedApiKeyId ||
                !gatewayBaseUrl.trim()
              }
              className="w-fit"
            >
              {applyGatewayMutation.isPending ? (
                <Loader2 className="size-4 animate-spin" />
              ) : (
                <Network className="size-4" />
              )}
              {isGatewayActive ? t("重新应用本地网关") : t("切换到本地网关")}
            </Button>
          </CardContent>
        </Card>
      </div>

      <details className="group rounded-xl border border-border/70 bg-card shadow-sm">
        <summary className="flex cursor-pointer list-none items-center justify-between gap-3 px-5 py-4">
          <div>
            <h2 className="text-base font-semibold">{t("高级与恢复")}</h2>
            <p className="mt-1 text-xs text-muted-foreground">
              {t("修改 profile 目录、gateway base_url、修复历史会话或恢复接管前配置。")}
            </p>
          </div>
          <ChevronDown className="size-4 text-muted-foreground transition-transform group-open:rotate-180" />
        </summary>
        <div className="grid gap-5 border-t border-border/60 px-5 py-5">
          <div className="grid gap-5 lg:grid-cols-2">
            <Card className="border-border/70">
              <CardHeader>
                <CardTitle>{t("Profile 目标目录")}</CardTitle>
                <CardDescription>
                  {t("默认使用 CODEX_HOME 或 service 用户的 ~/.codex。")}
                </CardDescription>
              </CardHeader>
              <CardContent className="grid gap-4">
                <div className="grid gap-2">
                  <Label htmlFor="codex-home">{t("Codex profile 目录")}</Label>
                  <div className="flex flex-col gap-2 sm:flex-row">
                    <Input
                      id="codex-home"
                      value={codexHomeInput}
                      onChange={(event) => setCodexHomeDraft(event.target.value)}
                      placeholder="~/.codex"
                      disabled={!isServiceReady || isMutating}
                    />
                    <Button
                      type="button"
                      variant="outline"
                      onClick={() => saveConfigMutation.mutate()}
                      disabled={!isServiceReady || isMutating || !codexHomeInput.trim()}
                    >
                      {saveConfigMutation.isPending ? (
                        <Loader2 className="size-4 animate-spin" />
                      ) : (
                        <Save className="size-4" />
                      )}
                      {t("保存")}
                    </Button>
                  </div>
                </div>
                <div className="grid gap-2 rounded-lg border bg-muted/30 p-3 text-xs text-muted-foreground">
                  <div className="flex justify-between gap-3">
                    <span>{t("auth.json")}</span>
                    <span className="truncate text-foreground">{status?.authPath || "-"}</span>
                  </div>
                  <div className="flex justify-between gap-3">
                    <span>{t("config.toml")}</span>
                    <span className="truncate text-foreground">{status?.configPath || "-"}</span>
                  </div>
                  <div className="flex justify-between gap-3">
                    <span>{t("CodexManager 管理文件")}</span>
                    <span className="truncate text-foreground">
                      {status?.managedStorageRoot || "-"}
                    </span>
                  </div>
                  <div className="flex justify-between gap-3">
                    <span>{t("管理标记")}</span>
                    <span className="truncate text-foreground">
                      {status?.markerPath || "-"}
                    </span>
                  </div>
                  <div className="flex justify-between gap-3">
                    <span>{t("可写")}</span>
                    <span className="text-foreground">
                      {status?.profileWritable ? t("是") : t("否或未知")}
                    </span>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="border-border/70">
              <CardHeader>
                <CardTitle>{t("Gateway base_url")}</CardTitle>
                <CardDescription>
                  {t("默认使用当前 Web 服务可访问的本地网关地址。")}
                </CardDescription>
              </CardHeader>
              <CardContent className="grid gap-2">
                <Label htmlFor="gateway-base-url">{t("OpenAI gateway base_url")}</Label>
                <div className="flex flex-col gap-2 sm:flex-row">
                  <Input
                    id="gateway-base-url"
                    value={gatewayBaseUrl}
                    onChange={(event) => setGatewayBaseUrlDraft(event.target.value)}
                    placeholder={defaultGatewayBaseUrl || "http://localhost:48760/v1"}
                    disabled={!isServiceReady || isMutating}
                  />
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => setGatewayBaseUrlDraft(defaultGatewayBaseUrl)}
                    disabled={!defaultGatewayBaseUrl || isMutating}
                  >
                    <KeyRound className="size-4" />
                    {t("使用当前网关")}
                  </Button>
                </div>
              </CardContent>
            </Card>
          </div>

          <Card className="border-border/70">
            <CardHeader>
              <CardTitle>{t("恢复与历史会话")}</CardTitle>
              <CardDescription>
                {t("切换模式时会自动修复历史会话 provider 元数据；Codex 运行中锁库时可手动重试。")}
              </CardDescription>
            </CardHeader>
            <CardContent className="grid gap-4">
              <div className="grid gap-2 rounded-lg border bg-muted/20 p-3 text-xs">
                <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                  <div>
                    <p className="font-medium text-foreground">{t("历史会话可见性")}</p>
                    <p className="text-muted-foreground">
                      {latestHistoryRepair
                        ? latestHistoryRepair.message
                        : t("切换 direct / gateway 时会自动修复历史会话的 provider 元数据。")}
                    </p>
                  </div>
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => repairHistoryMutation.mutate()}
                    disabled={!isServiceReady || isMutating || !codexHomeInput.trim()}
                  >
                    {repairHistoryMutation.isPending ? (
                      <Loader2 className="size-4 animate-spin" />
                    ) : (
                      <Wrench className="size-4" />
                    )}
                    {t("修复历史可见性")}
                  </Button>
                </div>
                {latestHistoryRepair ? (
                  <div className="grid gap-1 text-muted-foreground">
                    <span>
                      {t("目标 provider")}：{latestHistoryRepair.targetProvider || "-"}
                    </span>
                    <span>
                      {t("已修复 rollout / SQLite / session_index")}：
                      {latestHistoryRepair.changedRolloutFileCount} /{" "}
                      {latestHistoryRepair.updatedSqliteRowCount} /{" "}
                      {latestHistoryRepair.addedSessionIndexEntryCount}
                    </span>
                    {latestHistoryRepair.backupDir ? (
                      <span className="truncate">
                        {t("备份目录")}：{latestHistoryRepair.backupDir}
                      </span>
                    ) : null}
                    {latestHistoryRepair.warnings.length > 0 ? (
                      <span className="text-amber-600 dark:text-amber-400">
                        {t("警告")}：{latestHistoryRepair.warnings[0]}
                      </span>
                    ) : null}
                  </div>
                ) : null}
              </div>
              <div className="grid gap-3 rounded-lg border bg-muted/20 p-3 text-xs">
                <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                  <div className="min-w-0">
                    <p className="font-medium text-foreground">{t("历史修复备份")}</p>
                    <p className="mt-1 text-muted-foreground">
                      {t("备份保存在 CodexManager 数据目录，不再写入 Codex profile。")}
                    </p>
                  </div>
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => pruneHistoryBackupsMutation.mutate()}
                    disabled={!isServiceReady || isMutating || !codexHomeInput.trim()}
                    className="w-fit"
                  >
                    {pruneHistoryBackupsMutation.isPending ? (
                      <Loader2 className="size-4 animate-spin" />
                    ) : (
                      <Trash2 className="size-4" />
                    )}
                    {t("清理历史备份")}
                  </Button>
                </div>
                <div className="grid gap-2 text-muted-foreground sm:grid-cols-2">
                  <span className="truncate">
                    {t("备份目录")}：{status?.historyBackupRoot || "-"}
                  </span>
                  <span>
                    {t("数量 / 占用")}：{status?.historyBackupCount ?? 0} /{" "}
                    {formatBytes(status?.historyBackupBytes)}
                  </span>
                  <span className="sm:col-span-2">
                    {t("保留策略")}：
                    {t("最多 {count} 份，最多 {days} 天，至少保留最新 {min} 份", {
                      count:
                        status?.historyRetention.maxHistoryBackupsPerProfile ?? 3,
                      days: status?.historyRetention.maxHistoryBackupAgeDays ?? 7,
                      min:
                        status?.historyRetention.minHistoryBackupsPerProfile ?? 1,
                    })}
                  </span>
                </div>
              </div>
              <Separator />
              <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                <div className="text-xs text-muted-foreground">
                  {t("备份")}：{status?.hasBackup ? t("已保存") : t("暂无")}
                </div>
                <Button
                  type="button"
                  variant="destructive"
                  onClick={() => restoreMutation.mutate()}
                  disabled={!isServiceReady || isMutating || !status?.hasBackup}
                >
                  {restoreMutation.isPending ? (
                    <Loader2 className="size-4 animate-spin" />
                  ) : (
                    <RotateCcw className="size-4" />
                  )}
                  {t("恢复接管前配置")}
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      </details>
    </main>
  );
}
