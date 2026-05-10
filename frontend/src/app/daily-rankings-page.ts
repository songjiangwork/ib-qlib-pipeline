import { CommonModule, DecimalPipe } from '@angular/common';
import { Component, computed, inject, signal } from '@angular/core';
import { Router } from '@angular/router';

import { FrontendI18nService } from './frontend-i18n.service';
import { FrontendStateService } from './frontend-state.service';

@Component({
  standalone: true,
  selector: 'app-daily-rankings-page',
  imports: [CommonModule, DecimalPipe],
  templateUrl: './daily-rankings-page.html',
  styleUrl: './daily-rankings-page.css',
})
export class DailyRankingsPage {
  protected readonly i18n = inject(FrontendI18nService);
  protected readonly state = inject(FrontendStateService);
  private readonly router = inject(Router);
  private readonly sortKey = signal<
    'rank' | 'symbol' | 'score' | 'percentile' | 'entry_price' | 'oneDay' | 'latest'
  >('rank');
  private readonly sortDirection = signal<'asc' | 'desc'>('asc');
  protected readonly sortedRankingDates = computed(() =>
    [...this.state.rankingDates()].sort((a, b) => b.signal_date.localeCompare(a.signal_date)),
  );
  protected readonly canGoPreviousTradingDay = computed(() => {
    const index = this.selectedRankingIndex();
    return index >= 0 && index < this.sortedRankingDates().length - 1;
  });
  protected readonly canGoNextTradingDay = computed(() => this.selectedRankingIndex() > 0);

  protected readonly top20 = computed(() => {
    const rows = (this.state.selectedRunData()?.recommendations ?? []).filter((row) => row.rank <= 20);
    const symbolFilter = this.state.symbolFilter().trim().toUpperCase();
    const filtered = symbolFilter ? rows.filter((row) => row.symbol.includes(symbolFilter)) : rows;
    return [...filtered].sort((a, b) => this.compareRows(a, b));
  });

  private readonly selectedRankingIndex = computed(() => {
    const runId = this.state.selectedRankingRunId();
    if (runId === null) {
      return -1;
    }
    return this.sortedRankingDates().findIndex((item) => item.run_id === runId);
  });

  protected toggleSort(
    key: 'rank' | 'symbol' | 'score' | 'percentile' | 'entry_price' | 'oneDay' | 'latest',
  ): void {
    if (this.sortKey() === key) {
      this.sortDirection.set(this.sortDirection() === 'asc' ? 'desc' : 'asc');
      return;
    }
    this.sortKey.set(key);
    this.sortDirection.set(key === 'symbol' ? 'asc' : 'desc');
  }

  protected sortMarker(
    key: 'rank' | 'symbol' | 'score' | 'percentile' | 'entry_price' | 'oneDay' | 'latest',
  ): string {
    if (this.sortKey() !== key) {
      return '';
    }
    return this.sortDirection() === 'asc' ? ' ▲' : ' ▼';
  }

  protected async openSymbol(symbol: string): Promise<void> {
    await this.state.loadSymbolLifecycle(symbol);
    await this.router.navigate(['/symbols', symbol]);
  }

  protected async onPortfolioRunChange(value: string): Promise<void> {
    const portfolioRunId = Number(value);
    if (!Number.isFinite(portfolioRunId) || portfolioRunId <= 0) {
      return;
    }
    await this.state.selectPortfolioRun(portfolioRunId);
  }

  protected async onUniverseChange(value: string): Promise<void> {
    const universeId = Number(value);
    if (!Number.isFinite(universeId) || universeId <= 0) {
      return;
    }
    await this.state.selectUniverse(universeId);
  }

  protected async onRankingRunChange(value: string): Promise<void> {
    const runId = Number(value);
    if (!Number.isFinite(runId) || runId <= 0) {
      return;
    }
    await this.state.selectRankingRun(runId);
  }

  protected async goPreviousTradingDay(): Promise<void> {
    const index = this.selectedRankingIndex();
    if (index < 0 || index >= this.sortedRankingDates().length - 1) {
      return;
    }
    await this.state.selectRankingRun(this.sortedRankingDates()[index + 1].run_id);
  }

  protected async goNextTradingDay(): Promise<void> {
    const index = this.selectedRankingIndex();
    if (index <= 0) {
      return;
    }
    await this.state.selectRankingRun(this.sortedRankingDates()[index - 1].run_id);
  }

  private compareRows(
    a: { rank: number; symbol: string; score: number; percentile: number | null; entry_price: number | null; performance?: any },
    b: { rank: number; symbol: string; score: number; percentile: number | null; entry_price: number | null; performance?: any },
  ): number {
    const key = this.sortKey();
    const dir = this.sortDirection() === 'asc' ? 1 : -1;
    const valueA =
      key === 'oneDay'
        ? (a.performance?.['1d']?.return_pct ?? null)
        : key === 'latest'
          ? (a.performance?.['latest']?.return_pct ?? null)
          : (a as any)[key];
    const valueB =
      key === 'oneDay'
        ? (b.performance?.['1d']?.return_pct ?? null)
        : key === 'latest'
          ? (b.performance?.['latest']?.return_pct ?? null)
          : (b as any)[key];
    return this.compareValues(valueA, valueB) * dir;
  }

  private compareValues(a: string | number | null | undefined, b: string | number | null | undefined): number {
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
    return Number(a) - Number(b);
  }
}
