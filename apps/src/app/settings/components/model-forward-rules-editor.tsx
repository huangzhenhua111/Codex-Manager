"use client";

import { Plus, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { createEmptyModelForwardRule } from "@/app/settings/settings-page-helpers";

export interface ModelForwardRuleRow {
  pattern: string;
  target: string;
}

interface ModelForwardRulesEditorProps {
  rows: ModelForwardRuleRow[];
  sourcePlaceholder: string;
  targetPlaceholder: string;
  sourceLabel: string;
  targetLabel: string;
  addButtonLabel: string;
  deleteButtonLabel: string;
  onRowsChange: (updater: (rows: ModelForwardRuleRow[]) => ModelForwardRuleRow[]) => void;
  onCommit: () => void;
}

export function ModelForwardRulesEditor({
  rows,
  sourcePlaceholder,
  targetPlaceholder,
  sourceLabel,
  targetLabel,
  addButtonLabel,
  deleteButtonLabel,
  onRowsChange,
  onCommit,
}: ModelForwardRulesEditorProps) {
  return (
    <div
      className="grid max-w-3xl gap-3 rounded-lg border border-border/60 bg-background/40 p-3"
      onBlur={(event) => {
        const nextTarget = event.relatedTarget;
        if (
          nextTarget instanceof Node &&
          event.currentTarget.contains(nextTarget)
        ) {
          return;
        }
        onCommit();
      }}
    >
      <div className="hidden grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto] gap-2 px-1 text-[10px] font-medium text-muted-foreground md:grid">
        <span>{sourceLabel}</span>
        <span>{targetLabel}</span>
        <span />
      </div>
      <div className="grid gap-2">
        {rows.map((rule, index) => (
          <div
            key={index}
            className="grid gap-2 md:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto]"
          >
            <Input
              className="h-10 font-mono text-xs"
              aria-label={sourceLabel}
              placeholder={sourcePlaceholder}
              value={rule.pattern}
              onChange={(event) =>
                onRowsChange((currentRows) => {
                  const nextRows = [...currentRows];
                  nextRows[index] = {
                    ...nextRows[index],
                    pattern: event.target.value,
                  };
                  return nextRows;
                })
              }
            />
            <Input
              className="h-10 font-mono text-xs"
              aria-label={targetLabel}
              placeholder={targetPlaceholder}
              value={rule.target}
              onChange={(event) =>
                onRowsChange((currentRows) => {
                  const nextRows = [...currentRows];
                  nextRows[index] = {
                    ...nextRows[index],
                    target: event.target.value,
                  };
                  return nextRows;
                })
              }
            />
            <Button
              type="button"
              variant="ghost"
              size="icon"
              className="h-10 w-10 shrink-0"
              aria-label={deleteButtonLabel}
              onClick={() =>
                onRowsChange((currentRows) => {
                  const nextRows = currentRows.filter(
                    (_, rowIndex) => rowIndex !== index,
                  );
                  return nextRows.length > 0
                    ? nextRows
                    : [createEmptyModelForwardRule()];
                })
              }
            >
              <Trash2 className="h-4 w-4" />
            </Button>
          </div>
        ))}
      </div>
      <div>
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="gap-2"
          onClick={() =>
            onRowsChange((currentRows) => [
              ...currentRows.filter(
                (item) => item.pattern.length > 0 || item.target.length > 0,
              ),
              createEmptyModelForwardRule(),
            ])
          }
        >
          <Plus className="h-4 w-4" />
          {addButtonLabel}
        </Button>
      </div>
    </div>
  );
}
