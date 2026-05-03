import { CommonModule, DecimalPipe } from '@angular/common';
import { Component, computed, effect, inject } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';
import { ActivatedRoute } from '@angular/router';
import { map } from 'rxjs/operators';

import { FrontendStateService, PortfolioMark } from './frontend-state.service';

@Component({
  standalone: true,
  selector: 'app-symbol-detail-page',
  imports: [CommonModule, DecimalPipe],
  templateUrl: './symbol-detail-page.html',
  styleUrl: './symbol-detail-page.css',
})
export class SymbolDetailPage {
  protected readonly state = inject(FrontendStateService);
  private readonly route = inject(ActivatedRoute);
  private readonly routeSymbol = toSignal(
    this.route.paramMap.pipe(map((params) => params.get('symbol'))),
    { initialValue: this.route.snapshot.paramMap.get('symbol') },
  );

  protected readonly lifecycle = computed(() => this.state.selectedSymbolLifecycle());
  protected readonly headlineStats = computed(() => {
    const lots = this.lifecycle()?.lots ?? [];
    const closed = lots.filter((lot) => lot.status === 'closed');
    const open = lots.filter((lot) => lot.status === 'open');
    const realizedPnl = closed.reduce((sum, lot) => sum + (lot.realized_pnl ?? 0), 0);
    const latestMarks = lots
      .map((lot) => this.latestMark(lot.marks))
      .filter((mark): mark is PortfolioMark => mark !== null);
    const latestUnrealized = latestMarks.reduce((sum, mark) => sum + mark.unrealized_pnl, 0);
    return {
      lotCount: lots.length,
      closedCount: closed.length,
      openCount: open.length,
      realizedPnl,
      latestUnrealized,
    };
  });

  constructor() {
    effect(() => {
      const symbol = this.routeSymbol();
      if (symbol) {
        void this.state.loadSymbolLifecycle(symbol);
      }
    });
  }

  protected pnlClass(value: number | null | undefined): string {
    if (value === null || value === undefined) {
      return '';
    }
    return value >= 0 ? 'up' : 'down';
  }

  protected latestMark(marks: PortfolioMark[]): PortfolioMark | null {
    return marks.length ? marks[marks.length - 1] : null;
  }
}
