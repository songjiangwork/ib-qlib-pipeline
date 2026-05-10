import { CommonModule, DecimalPipe } from '@angular/common';
import { Component, computed, inject, signal } from '@angular/core';
import { Router } from '@angular/router';

import { FrontendI18nService } from './frontend-i18n.service';
import { FrontendStateService, PortfolioLot } from './frontend-state.service';

interface CompareRow {
  symbol: string;
  timesEntered: number;
  closedLots: number;
  openLots: number;
  avgHoldDays: number | null;
  winRatePct: number | null;
  avgReturnPct: number | null;
  totalRealizedPnl: number;
  latestStatus: 'open' | 'closed' | 'mixed';
}

interface PortfolioRunRow {
  id: number;
  name: string;
  modelKey: string;
  modelName: string;
  workflowBase: string;
  lotCount: number;
  closedLots: number;
  openLots: number;
  avgHoldDays: number | null;
  winRatePct: number | null;
  avgReturnPct: number | null;
  totalRealizedPnl: number | null;
  isSelected: boolean;
}

@Component({
  standalone: true,
  selector: 'app-compare-page',
  imports: [CommonModule, DecimalPipe],
  templateUrl: './compare-page.html',
  styleUrl: './compare-page.css',
})
export class ComparePage {
  protected readonly state = inject(FrontendStateService);
  protected readonly i18n = inject(FrontendI18nService);
  private readonly router = inject(Router);
  private readonly runSortKey = signal<keyof PortfolioRunRow>('totalRealizedPnl');
  private readonly runSortDirection = signal<'asc' | 'desc'>('desc');
  private readonly symbolSortKey = signal<keyof CompareRow>('totalRealizedPnl');
  private readonly symbolSortDirection = signal<'asc' | 'desc'>('desc');

  protected readonly compareRows = computed(() => {
    return this.state.compareSymbols()
      .map((symbol) => this.buildRow(symbol))
      .filter((row): row is CompareRow => row !== null)
      .sort((a, b) => this.compareByKey(a, b, this.symbolSortKey(), this.symbolSortDirection()));
  });

  protected readonly portfolioRunRows = computed(() => {
    return [...this.state.portfolioRuns()]
      .map((run) => ({
        id: run.id,
        name: run.name,
        modelKey: run.model_key || 'n/a',
        modelName: run.model_name || 'N/A',
        workflowBase: this.workflowLabel(run.workflow_base),
        lotCount: run.lot_count,
        closedLots: run.closed_lot_count ?? 0,
        openLots: run.open_lot_count ?? 0,
        avgHoldDays: run.avg_hold_days ?? null,
        winRatePct: run.win_rate_pct ?? null,
        avgReturnPct: run.avg_return_pct ?? null,
        totalRealizedPnl: run.total_realized_pnl ?? null,
        isSelected: this.state.selectedPortfolioRunId() === run.id,
      }))
      .sort((a, b) => this.compareByKey(a, b, this.runSortKey(), this.runSortDirection()));
  });

  protected toggleSymbol(symbol: string): void {
    this.state.toggleCompareSymbol(symbol);
  }

  protected clearAll(): void {
    this.state.clearCompareSymbols();
  }

  protected async openSymbol(symbol: string): Promise<void> {
    await this.state.loadSymbolLifecycle(symbol);
    await this.router.navigate(['/symbols', symbol]);
  }

  protected async selectPortfolioRun(portfolioRunId: number): Promise<void> {
    await this.state.selectPortfolioRun(portfolioRunId);
  }

  protected toggleRunSort(key: keyof PortfolioRunRow): void {
    if (this.runSortKey() === key) {
      this.runSortDirection.set(this.runSortDirection() === 'asc' ? 'desc' : 'asc');
      return;
    }
    this.runSortKey.set(key);
    this.runSortDirection.set(key === 'name' || key === 'modelName' || key === 'workflowBase' ? 'asc' : 'desc');
  }

  protected toggleSymbolSort(key: keyof CompareRow): void {
    if (this.symbolSortKey() === key) {
      this.symbolSortDirection.set(this.symbolSortDirection() === 'asc' ? 'desc' : 'asc');
      return;
    }
    this.symbolSortKey.set(key);
    this.symbolSortDirection.set(key === 'symbol' || key === 'latestStatus' ? 'asc' : 'desc');
  }

  protected runSortMarker(key: keyof PortfolioRunRow): string {
    return this.sortMarker(this.runSortKey(), this.runSortDirection(), key);
  }

  protected symbolSortMarker(key: keyof CompareRow): string {
    return this.sortMarker(this.symbolSortKey(), this.symbolSortDirection(), key);
  }

  protected pnlClass(value: number | null | undefined): string {
    if (value === null || value === undefined) {
      return '';
    }
    return value >= 0 ? 'up' : 'down';
  }

  protected statusLabel(status: CompareRow['latestStatus']): string {
    if (status === 'mixed') {
      return this.i18n.t('mixed');
    }
    return this.i18n.t(status);
  }

  private buildRow(symbol: string): CompareRow | null {
    const lots = this.state.portfolioLots().filter((lot) => lot.symbol === symbol);
    if (!lots.length) {
      return null;
    }
    const closed = lots.filter((lot) => lot.status === 'closed');
    const open = lots.filter((lot) => lot.status === 'open');
    const closedWithReturn = closed.filter((lot) => lot.realized_return_pct !== null);
    const wins = closedWithReturn.filter((lot) => (lot.realized_return_pct ?? 0) > 0).length;
    const avgHoldDays = this.average(closed.map((lot) => this.holdDays(lot)).filter((days) => days !== null));
    const avgReturnPct = this.average(closedWithReturn.map((lot) => lot.realized_return_pct));
    const totalRealizedPnl = closed.reduce((sum, lot) => sum + (lot.realized_pnl ?? 0), 0);
    const latestStatus: CompareRow['latestStatus'] =
      open.length && closed.length ? 'mixed' : open.length ? 'open' : 'closed';

    return {
      symbol,
      timesEntered: lots.length,
      closedLots: closed.length,
      openLots: open.length,
      avgHoldDays,
      winRatePct: closedWithReturn.length ? (wins / closedWithReturn.length) * 100 : null,
      avgReturnPct,
      totalRealizedPnl,
      latestStatus,
    };
  }

  private holdDays(lot: PortfolioLot): number | null {
    if (!lot.exit_trade_date) {
      return null;
    }
    const start = new Date(lot.entry_trade_date);
    const end = new Date(lot.exit_trade_date);
    return Math.round((end.getTime() - start.getTime()) / 86400000);
  }

  private average(values: Array<number | null>): number | null {
    const actual = values.filter((value): value is number => value !== null);
    if (!actual.length) {
      return null;
    }
    return actual.reduce((sum, value) => sum + value, 0) / actual.length;
  }

  private workflowLabel(value: string | null | undefined): string {
    if (!value) {
      return 'N/A';
    }
    const parts = value.split('/');
    return parts[parts.length - 1] || value;
  }

  private sortMarker(currentKey: string, currentDirection: 'asc' | 'desc', key: string): string {
    if (currentKey !== key) {
      return '';
    }
    return currentDirection === 'asc' ? ' ▲' : ' ▼';
  }

  private compareByKey<T extends Record<string, any>>(
    a: T,
    b: T,
    key: keyof T,
    direction: 'asc' | 'desc',
  ): number {
    const dir = direction === 'asc' ? 1 : -1;
    return this.compareValues(a[key], b[key]) * dir;
  }

  private compareValues(a: string | number | boolean | null | undefined, b: string | number | boolean | null | undefined): number {
    if (a == null && b == null) {
      return 0;
    }
    if (a == null) {
      return 1;
    }
    if (b == null) {
      return -1;
    }
    if (typeof a === 'string' && typeof b === 'string') {
      return a.localeCompare(b);
    }
    if (typeof a === 'boolean' && typeof b === 'boolean') {
      return Number(a) - Number(b);
    }
    return Number(a) - Number(b);
  }
}
