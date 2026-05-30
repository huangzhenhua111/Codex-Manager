"use client";

import { Check, Palette } from "lucide-react";
import { APPEARANCE_PRESETS } from "@/lib/appearance";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { cn } from "@/lib/utils";
import { THEMES } from "@/app/settings/settings-page-helpers";

type TranslateFn = (key: string) => string;

interface AppearanceTabContentProps {
  t: TranslateFn;
  theme: string | undefined;
  appearancePreset: string | null | undefined;
  onThemeChange: (nextTheme: string) => void;
  onAppearancePresetChange: (nextPreset: string) => void;
}

export function AppearanceTabContent({
  t,
  theme,
  appearancePreset,
  onThemeChange,
  onAppearancePresetChange,
}: AppearanceTabContentProps) {
  return (
    <>
      <Card className="glass-card shadow-sm">
        <CardHeader>
          <div className="flex items-center gap-2">
            <Palette className="h-4 w-4 text-primary" />
            <CardTitle className="text-base">{t("样式版本")}</CardTitle>
          </div>
          <CardDescription>{t("在渐变版本和默认版本之间切换")}</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid gap-3 md:grid-cols-2">
            {APPEARANCE_PRESETS.map((item) => {
              const isActive = appearancePreset === item.id;
              return (
                <Button
                  key={item.id}
                  type="button"
                  variant="outline"
                  onClick={() => onAppearancePresetChange(item.id)}
                  className={cn(
                    "group relative h-auto justify-start rounded-xl p-4 text-left transition-all duration-300",
                    isActive
                      ? "border-primary bg-primary/10 shadow-sm ring-1 ring-primary"
                      : "border-border/60 bg-background/50 hover:bg-accent/30",
                  )}
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="space-y-1.5">
                      <div className="text-sm font-semibold">{t(item.name)}</div>
                      <p className="text-xs leading-5 text-muted-foreground">
                        {t(item.description)}
                      </p>
                    </div>
                    {isActive ? (
                      <div className="rounded-full bg-primary p-1 text-primary-foreground shadow-sm">
                        <Check className="h-3 w-3" />
                      </div>
                    ) : null}
                  </div>
                  <div className="mt-3 flex items-end gap-2.5">
                    <div
                      className={cn(
                        "h-14 flex-1 rounded-xl border",
                        item.id === "modern"
                          ? "border-primary/20 bg-accent/50"
                          : "border-border/70 bg-muted/70",
                      )}
                    />
                    <div className="flex w-16 flex-col gap-1.5">
                      <div
                        className={cn(
                          "h-4 rounded-lg border",
                          item.id === "modern"
                            ? "border-primary/15 bg-card shadow-sm"
                            : "border-border/70 bg-card",
                        )}
                      />
                      <div
                        className={cn(
                          "h-4 rounded-lg border",
                          item.id === "modern"
                            ? "border-primary/15 bg-card/80 shadow-sm"
                            : "border-border/70 bg-card/80",
                        )}
                      />
                    </div>
                  </div>
                </Button>
              );
            })}
          </div>
        </CardContent>
      </Card>

      <Card className="glass-card shadow-sm">
        <CardHeader>
          <div className="flex items-center gap-2">
            <Palette className="h-4 w-4 text-primary" />
            <CardTitle className="text-base">{t("界面主题")}</CardTitle>
          </div>
          <CardDescription>
            {t("选择您喜爱的配色方案，适配不同工作心情")}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 xl:grid-cols-12">
            {THEMES.map((item) => (
              <Button
                key={item.id}
                type="button"
                variant="ghost"
                onClick={() => onThemeChange(item.id)}
                className={cn(
                  "group relative h-auto flex-col items-center gap-2.5 rounded-xl border p-4 transition-all duration-300 hover:bg-accent/40",
                  theme === item.id
                    ? "border-primary bg-primary/10 shadow-sm ring-1 ring-primary"
                    : "border-transparent bg-muted/20 hover:bg-accent/40",
                )}
              >
                <div
                  className="h-10 w-10 rounded-full border-2 border-white/20 shadow-sm"
                  style={{ backgroundColor: item.color }}
                />
                <span
                  className={cn(
                    "whitespace-nowrap text-[10px] font-semibold transition-colors",
                    theme === item.id
                      ? "text-primary"
                      : "text-muted-foreground group-hover:text-foreground",
                  )}
                >
                  {t(item.name)}
                </span>
                {theme === item.id ? (
                  <div className="absolute right-2 top-2 rounded-full bg-primary p-0.5 text-primary-foreground shadow-sm">
                    <Check className="h-2.5 w-2.5" />
                  </div>
                ) : null}
              </Button>
            ))}
          </div>
        </CardContent>
      </Card>
    </>
  );
}
