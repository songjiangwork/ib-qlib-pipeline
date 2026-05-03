import { CommonModule, DecimalPipe } from '@angular/common';
import { Component, computed, inject } from '@angular/core';
import { Router } from '@angular/router';

import { FrontendStateService } from './frontend-state.service';

@Component({
  standalone: true,
  selector: 'app-daily-rankings-page',
  imports: [CommonModule, DecimalPipe],
  templateUrl: './daily-rankings-page.html',
  styleUrl: './daily-rankings-page.css',
})
export class DailyRankingsPage {
  protected readonly state = inject(FrontendStateService);
  private readonly router = inject(Router);

  protected readonly top20 = computed(() => {
    const rows = (this.state.selectedRunData()?.recommendations ?? []).filter((row) => row.rank <= 20);
    const symbolFilter = this.state.symbolFilter().trim().toUpperCase();
    if (!symbolFilter) {
      return rows;
    }
    return rows.filter((row) => row.symbol.includes(symbolFilter));
  });

  protected async openSymbol(symbol: string): Promise<void> {
    await this.state.loadSymbolLifecycle(symbol);
    await this.router.navigate(['/symbols', symbol]);
  }
}
