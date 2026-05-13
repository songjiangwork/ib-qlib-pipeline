import { CommonModule, DecimalPipe } from '@angular/common';
import { Component, computed, effect, inject, signal } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';
import { ActivatedRoute, Router } from '@angular/router';
import { map } from 'rxjs/operators';

import { FrontendI18nService } from './frontend-i18n.service';
import { FrontendStateService, PortfolioRunSummary } from './frontend-state.service';

type PortfolioSortKey =
  | 'id'
  | 'name'
  | 'universe_name'
  | 'model_name'
  | 'closed_lot_count'
  | 'open_lot_count'
  | 'win_rate_pct'
  | 'total_realized_pnl';

@Component({
  standalone: true,
  selector: 'app-portfolio-page',
  imports: [CommonModule, DecimalPipe],
  templateUrl: './portfolio-page.html',
  styleUrl: './portfolio-page.css',
})
export class PortfolioPage {
  protected readonly state = inject(FrontendStateService);
  protected readonly i18n = inject(FrontendI18nService);
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly sortKey = signal<PortfolioSortKey>('total_realized_pnl');
  private readonly sortDirection = signal<'asc' | 'desc'>('desc');
  private readonly queryUniverseId = toSignal(
    this.route.queryParamMap.pipe(map((params) => params.get('u'))),
    { initialValue: this.route.snapshot.queryParamMap.get('u') },
  );
  private readonly queryPortfolioRunId = toSignal(
    this.route.queryParamMap.pipe(map((params) => params.get('p'))),
    { initialValue: this.route.snapshot.queryParamMap.get('p') },
  );
  private applyingQueryState = false;
  private lastAppliedQueryKey: string | null = null;

  protected readonly portfolioRows = computed(() =>
    [...this.state.portfolioRuns()].sort((a, b) => this.compareRows(a, b)),
  );

  protected readonly totalOpenLots = computed(() =>
    this.state.portfolioRuns().reduce((sum, row) => sum + (row.open_lot_count ?? 0), 0),
  );

  protected readonly totalClosedLots = computed(() =>
    this.state.portfolioRuns().reduce((sum, row) => sum + (row.closed_lot_count ?? 0), 0),
  );

  protected readonly selectedPortfolioName = computed(() => {
    const run = this.state.selectedPortfolioRun();
    return run ? `#${run.id} ${run.name}` : 'N/A';
  });

  constructor() {
    effect(() => {
      const initialized = this.state.initialized();
      const universe = this.queryUniverseId();
      const portfolio = this.queryPortfolioRunId();
      if (!initialized) {
        return;
      }
      const key = `${universe ?? ''}|${portfolio ?? ''}`;
      if (this.applyingQueryState || this.lastAppliedQueryKey === key) {
        return;
      }
      void this.applyQueryState(universe, portfolio, key);
    });

    effect(() => {
      if (!this.state.initialized() || this.applyingQueryState) {
        return;
      }
      const desired = {
        u: this.state.selectedUniverseId() ?? null,
        p: this.state.selectedPortfolioRunId() ?? null,
      };
      const current = {
        u: this.queryUniverseId() ? Number(this.queryUniverseId()) : null,
        p: this.queryPortfolioRunId() ? Number(this.queryPortfolioRunId()) : null,
      };
      if (desired.u === current.u && desired.p === current.p) {
        return;
      }
      void this.router.navigate([], {
        relativeTo: this.route,
        queryParams: desired,
        replaceUrl: true,
      });
    });
  }

  protected toggleSort(key: PortfolioSortKey): void {
    if (this.sortKey() === key) {
      this.sortDirection.set(this.sortDirection() === 'asc' ? 'desc' : 'asc');
      return;
    }
    this.sortKey.set(key);
    this.sortDirection.set(key === 'name' || key === 'universe_name' || key === 'model_name' ? 'asc' : 'desc');
  }

  protected sortMarker(key: PortfolioSortKey): string {
    if (this.sortKey() !== key) {
      return '';
    }
    return this.sortDirection() === 'asc' ? ' ▲' : ' ▼';
  }

  protected pnlClass(value: number | null | undefined): string {
    if (value === null || value === undefined) {
      return '';
    }
    return value >= 0 ? 'up' : 'down';
  }

  protected async openPortfolio(runId: number): Promise<void> {
    await this.state.selectPortfolioRun(runId);
    await this.router.navigate(['/rankings'], {
      queryParams: {
        u: this.state.selectedUniverseId() ?? null,
        p: this.state.selectedPortfolioRunId() ?? null,
        r: this.state.selectedRankingRunId() ?? null,
      },
    });
  }

  private async applyQueryState(
    universeParam: string | null,
    portfolioParam: string | null,
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
      this.lastAppliedQueryKey = key;
    } finally {
      this.applyingQueryState = false;
    }
  }

  private compareRows(a: PortfolioRunSummary, b: PortfolioRunSummary): number {
    const dir = this.sortDirection() === 'asc' ? 1 : -1;
    const key = this.sortKey();
    return this.compareValues((a as any)[key], (b as any)[key]) * dir;
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
