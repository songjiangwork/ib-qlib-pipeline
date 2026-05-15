import { CommonModule, DecimalPipe } from '@angular/common';
import { Component, computed, effect, inject, signal } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';
import { ActivatedRoute, Router } from '@angular/router';
import { map } from 'rxjs/operators';

import { FrontendI18nService } from './frontend-i18n.service';
import { FrontendStateService, PortfolioLot } from './frontend-state.service';

interface CompareRow {
  symbol: string;
  displayName: string | null;
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
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly symbolFilter = signal('');
  private readonly symbolSortKey = signal<keyof CompareRow>('totalRealizedPnl');
  private readonly symbolSortDirection = signal<'asc' | 'desc'>('desc');
  private readonly queryUniverseId = toSignal(
    this.route.queryParamMap.pipe(map((params) => params.get('u'))),
    { initialValue: this.route.snapshot.queryParamMap.get('u') },
  );
  private readonly queryPortfolioRunId = toSignal(
    this.route.queryParamMap.pipe(map((params) => params.get('p'))),
    { initialValue: this.route.snapshot.queryParamMap.get('p') },
  );
  private readonly querySymbols = toSignal(
    this.route.queryParamMap.pipe(map((params) => params.get('symbols'))),
    { initialValue: this.route.snapshot.queryParamMap.get('symbols') },
  );
  private applyingQueryState = false;
  private lastAppliedQueryKey: string | null = null;

  protected readonly filteredSymbolsInRun = computed(() => {
    const filter = this.symbolFilter().trim().toUpperCase();
    const symbols = this.state.symbolsInRun();
    return filter ? symbols.filter((symbol) => symbol.includes(filter)) : symbols;
  });

  protected readonly compareRows = computed(() => {
    return this.state.compareSymbols()
      .map((symbol) => this.buildRow(symbol))
      .filter((row): row is CompareRow => row !== null)
      .sort((a, b) => this.compareByKey(a, b, this.symbolSortKey(), this.symbolSortDirection()));
  });

  constructor() {
    effect(() => {
      const initialized = this.state.initialized();
      const universe = this.queryUniverseId();
      const portfolio = this.queryPortfolioRunId();
      const symbols = this.querySymbols();
      if (!initialized) {
        return;
      }
      const key = `${universe ?? ''}|${portfolio ?? ''}|${symbols ?? ''}`;
      if (this.applyingQueryState || this.lastAppliedQueryKey === key) {
        return;
      }
      void this.applyQueryState(universe, portfolio, symbols, key);
    });

    effect(() => {
      if (!this.state.initialized() || this.applyingQueryState) {
        return;
      }
      const desired = {
        u: this.state.selectedUniverseId() ?? null,
        p: this.state.selectedPortfolioRunId() ?? null,
        symbols: this.state.compareSymbols().length ? this.state.compareSymbols().join(',') : null,
      };
      const current = {
        u: this.queryUniverseId() ? Number(this.queryUniverseId()) : null,
        p: this.queryPortfolioRunId() ? Number(this.queryPortfolioRunId()) : null,
        symbols: this.querySymbols() || null,
      };
      if (desired.u === current.u && desired.p === current.p && desired.symbols === current.symbols) {
        return;
      }
      void this.router.navigate([], {
        relativeTo: this.route,
        queryParams: desired,
        replaceUrl: true,
      });
    });
  }

  protected toggleSymbol(symbol: string): void {
    this.state.toggleCompareSymbol(symbol);
  }

  protected clearAll(): void {
    this.state.clearCompareSymbols();
  }

  protected async openSymbol(symbol: string): Promise<void> {
    await this.state.loadSymbolLifecycle(symbol);
    await this.router.navigate(['/symbols', symbol], {
      queryParams: {
        u: this.state.selectedUniverseId() ?? null,
        p: this.state.selectedPortfolioRunId() ?? null,
      },
    });
  }

  protected async selectPortfolioRun(portfolioRunId: number): Promise<void> {
    await this.state.selectPortfolioRun(portfolioRunId);
  }

  protected async onPortfolioRunChange(value: string): Promise<void> {
    const portfolioRunId = Number(value);
    if (!Number.isFinite(portfolioRunId) || portfolioRunId <= 0) {
      return;
    }
    await this.selectPortfolioRun(portfolioRunId);
  }

  protected displaySymbol(symbol: string): string {
    const displayName = this.state.displayNameForSymbol(symbol);
    return displayName ? `${displayName} · ${symbol}` : symbol;
  }

  protected setSymbolFilter(value: string): void {
    this.symbolFilter.set(value);
  }

  protected toggleSymbolSort(key: keyof CompareRow): void {
    if (this.symbolSortKey() === key) {
      this.symbolSortDirection.set(this.symbolSortDirection() === 'asc' ? 'desc' : 'asc');
      return;
    }
    this.symbolSortKey.set(key);
    this.symbolSortDirection.set(key === 'symbol' || key === 'latestStatus' ? 'asc' : 'desc');
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
      displayName: this.state.displayNameForSymbol(symbol),
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

  private async applyQueryState(
    universeParam: string | null,
    portfolioParam: string | null,
    symbolsParam: string | null,
    key: string,
  ): Promise<void> {
    this.applyingQueryState = true;
    try {
      const universeId = universeParam ? Number(universeParam) : null;
      const portfolioRunId = portfolioParam ? Number(portfolioParam) : null;

      if (universeId && Number.isFinite(universeId) && universeId > 0 && this.state.selectedUniverseId() !== universeId) {
        await this.state.selectUniverse(universeId);
      }
      if (
        portfolioRunId &&
        Number.isFinite(portfolioRunId) &&
        portfolioRunId > 0 &&
        this.state.selectedPortfolioRunId() !== portfolioRunId &&
        this.state.portfolioRuns().some((run) => run.id === portfolioRunId)
      ) {
        await this.state.selectPortfolioRun(portfolioRunId);
      }

      const available = new Set(this.state.symbolsInRun());
      const symbols = (symbolsParam ?? '')
        .split(',')
        .map((symbol) => symbol.trim().toUpperCase())
        .filter((symbol) => symbol && available.has(symbol))
        .slice(0, 5);
      this.state.setCompareSymbols(symbols);
      this.lastAppliedQueryKey = key;
    } finally {
      this.applyingQueryState = false;
    }
  }
}
