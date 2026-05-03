import { CommonModule, DecimalPipe } from '@angular/common';
import { Component, computed, inject } from '@angular/core';
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

  protected readonly compareRows = computed(() => {
    return this.state.compareSymbols()
      .map((symbol) => this.buildRow(symbol))
      .filter((row): row is CompareRow => row !== null);
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
}
