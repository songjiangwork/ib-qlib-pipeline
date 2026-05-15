import { CommonModule, DecimalPipe } from '@angular/common';
import { Component, computed, effect, inject, signal } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';
import { ActivatedRoute, Router } from '@angular/router';
import { map } from 'rxjs/operators';

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
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly sortKey = signal<
    'rank' | 'symbol' | 'display_name' | 'percentile' | 'entry_price' | 'oneDay' | 'latest'
  >('rank');
  private readonly sortDirection = signal<'asc' | 'desc'>('asc');
  protected readonly sortedRankingDates = computed(() =>
    [...this.state.rankingDates()].sort((a, b) => b.signal_date.localeCompare(a.signal_date)),
  );
  protected readonly effectiveUniverseId = computed(() => {
    const raw = this.queryUniverseId();
    const parsed = raw ? Number(raw) : null;
    return parsed && Number.isFinite(parsed) && parsed > 0 ? parsed : this.state.selectedUniverseId();
  });
  protected readonly effectivePortfolioRunId = computed(() => {
    const raw = this.queryPortfolioRunId();
    const parsed = raw ? Number(raw) : null;
    return parsed && Number.isFinite(parsed) && parsed > 0 ? parsed : this.state.selectedPortfolioRunId();
  });
  protected readonly effectiveRankingRunId = computed(() => {
    const raw = this.queryRankingRunId();
    const parsed = raw ? Number(raw) : null;
    return parsed && Number.isFinite(parsed) && parsed > 0 ? parsed : this.state.selectedRankingRunId();
  });
  protected readonly availablePortfolioRuns = computed(() => {
    const universeId = this.effectiveUniverseId();
    if (universeId === null) {
      return this.state.portfolioRuns();
    }
    return this.state.portfolioRuns().filter((run) => run.universe_id === universeId);
  });
  protected readonly currentPortfolioRunName = computed(() => {
    const run = this.state.selectedPortfolioRun();
    return run ? `#${run.id} ${run.name}` : 'N/A';
  });
  protected readonly portfolioMetrics = computed(() => {
    const run = this.state.selectedPortfolioRun();
    if (!run) {
      return [];
    }
    return [
      { label: this.i18n.t('universe'), value: run.universe_name || 'N/A' },
      { label: this.i18n.t('model'), value: run.model_name || 'N/A', badge: run.model_key || null },
      { label: this.i18n.t('strategy'), value: run.strategy || 'N/A' },
      { label: this.i18n.t('openLots'), value: String(run.open_lot_count ?? 0) },
      { label: this.i18n.t('closedLots'), value: String(run.closed_lot_count ?? 0) },
      {
        label: this.i18n.t('realizedPnl'),
        value:
          run.total_realized_pnl === null || run.total_realized_pnl === undefined
            ? 'N/A'
            : run.total_realized_pnl.toFixed(2),
      },
    ];
  });
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
    const runId = this.effectiveRankingRunId();
    if (runId === null) {
      return -1;
    }
    return this.sortedRankingDates().findIndex((item) => item.run_id === runId);
  });

  protected selectValue(value: number | null | undefined): string {
    return value === null || value === undefined ? '' : `${value}`;
  }

  private readonly queryUniverseId = toSignal(
    this.route.queryParamMap.pipe(map((params) => params.get('u'))),
    { initialValue: this.route.snapshot.queryParamMap.get('u') },
  );
  private readonly queryPortfolioRunId = toSignal(
    this.route.queryParamMap.pipe(map((params) => params.get('p'))),
    { initialValue: this.route.snapshot.queryParamMap.get('p') },
  );
  private readonly queryRankingRunId = toSignal(
    this.route.queryParamMap.pipe(map((params) => params.get('r'))),
    { initialValue: this.route.snapshot.queryParamMap.get('r') },
  );
  private applyingQueryState = false;
  private lastAppliedQueryKey: string | null = null;

  protected toggleSort(
    key: 'rank' | 'symbol' | 'display_name' | 'percentile' | 'entry_price' | 'oneDay' | 'latest',
  ): void {
    if (this.sortKey() === key) {
      this.sortDirection.set(this.sortDirection() === 'asc' ? 'desc' : 'asc');
      return;
    }
    this.sortKey.set(key);
    this.sortDirection.set(key === 'symbol' ? 'asc' : 'desc');
  }

  protected sortMarker(
    key: 'rank' | 'symbol' | 'display_name' | 'percentile' | 'entry_price' | 'oneDay' | 'latest',
  ): string {
    if (this.sortKey() !== key) {
      return '';
    }
    return this.sortDirection() === 'asc' ? ' ▲' : ' ▼';
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

  protected async onPortfolioRunChange(value: string): Promise<void> {
    const portfolioRunId = Number(value);
    if (!Number.isFinite(portfolioRunId) || portfolioRunId <= 0) {
      return;
    }
    const run = this.state.portfolioRuns().find((item) => item.id === portfolioRunId) ?? null;
    await this.router.navigate([], {
      relativeTo: this.route,
      queryParams: {
        u: run?.universe_id ?? this.effectiveUniverseId(),
        p: portfolioRunId,
        r: null,
      },
    });
  }

  protected async onUniverseChange(value: string): Promise<void> {
    const universeId = Number(value);
    if (!Number.isFinite(universeId) || universeId <= 0) {
      return;
    }
    await this.router.navigate([], {
      relativeTo: this.route,
      queryParams: {
        u: universeId,
        p: null,
        r: null,
      },
    });
  }

  protected async onRankingRunChange(value: string): Promise<void> {
    const runId = Number(value);
    if (!Number.isFinite(runId) || runId <= 0) {
      return;
    }
    await this.router.navigate([], {
      relativeTo: this.route,
      queryParams: {
        u: this.effectiveUniverseId(),
        p: this.effectivePortfolioRunId(),
        r: runId,
      },
    });
  }

  protected async goPreviousTradingDay(): Promise<void> {
    const index = this.selectedRankingIndex();
    if (index < 0 || index >= this.sortedRankingDates().length - 1) {
      return;
    }
    await this.router.navigate([], {
      relativeTo: this.route,
      queryParams: {
        u: this.effectiveUniverseId(),
        p: this.effectivePortfolioRunId(),
        r: this.sortedRankingDates()[index + 1].run_id,
      },
    });
  }

  protected async goNextTradingDay(): Promise<void> {
    const index = this.selectedRankingIndex();
    if (index <= 0) {
      return;
    }
    await this.router.navigate([], {
      relativeTo: this.route,
      queryParams: {
        u: this.effectiveUniverseId(),
        p: this.effectivePortfolioRunId(),
        r: this.sortedRankingDates()[index - 1].run_id,
      },
    });
  }

  private compareRows(
    a: { rank: number; symbol: string; display_name?: string | null; percentile: number | null; entry_price: number | null; performance?: any },
    b: { rank: number; symbol: string; display_name?: string | null; percentile: number | null; entry_price: number | null; performance?: any },
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

  constructor() {
    effect(() => {
      const initialized = this.state.initialized();
      const universe = this.queryUniverseId();
      const portfolio = this.queryPortfolioRunId();
      const ranking = this.queryRankingRunId();
      if (!initialized) {
        return;
      }
      const key = `${universe ?? ''}|${portfolio ?? ''}|${ranking ?? ''}`;
      if (this.applyingQueryState || this.lastAppliedQueryKey === key) {
        return;
      }
      void this.applyQueryState(universe, portfolio, ranking, key);
    });

    effect(() => {
      if (!this.state.initialized() || this.applyingQueryState) {
        return;
      }
      const desired = {
        u: this.state.selectedUniverseId() ?? null,
        p: this.state.selectedPortfolioRunId() ?? null,
        r: this.state.selectedRankingRunId() ?? null,
      };
      const current = {
        u: this.queryUniverseId() ? Number(this.queryUniverseId()) : null,
        p: this.queryPortfolioRunId() ? Number(this.queryPortfolioRunId()) : null,
        r: this.queryRankingRunId() ? Number(this.queryRankingRunId()) : null,
      };
      if (desired.u === current.u && desired.p === current.p && desired.r === current.r) {
        return;
      }
      void this.router.navigate([], {
        relativeTo: this.route,
        queryParams: desired,
        replaceUrl: true,
      });
    });
  }

  private async applyQueryState(
    universeParam: string | null,
    portfolioParam: string | null,
    rankingParam: string | null,
    key: string,
  ): Promise<void> {
    this.applyingQueryState = true;
    try {
      const universeId = universeParam ? Number(universeParam) : null;
      const portfolioRunId = portfolioParam ? Number(portfolioParam) : null;
      const rankingRunId = rankingParam ? Number(rankingParam) : null;

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
      if (
        rankingRunId &&
        Number.isFinite(rankingRunId) &&
        rankingRunId > 0 &&
        this.state.selectedRankingRunId() !== rankingRunId &&
        this.state.rankingDates().some((item) => item.run_id === rankingRunId)
      ) {
        await this.state.selectRankingRun(rankingRunId);
      }
      this.lastAppliedQueryKey = key;
    } finally {
      this.applyingQueryState = false;
    }
  }
}
