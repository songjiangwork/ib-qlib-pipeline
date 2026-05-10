import { CommonModule } from '@angular/common';
import { Component, inject } from '@angular/core';
import { Router, RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';

import { FrontendI18nService, Language } from './frontend-i18n.service';
import { FrontendStateService } from './frontend-state.service';

@Component({
  selector: 'app-root',
  imports: [CommonModule, RouterOutlet, RouterLink, RouterLinkActive],
  templateUrl: './app.html',
  styleUrl: './app.css',
})
export class App {
  protected readonly i18n = inject(FrontendI18nService);
  protected readonly state = inject(FrontendStateService);
  private readonly router = inject(Router);
  private rankingDateFilterTimer: ReturnType<typeof setTimeout> | null = null;

  constructor() {
    void this.state.loadInitial();
  }

  protected async onPortfolioRunClick(portfolioRunId: number): Promise<void> {
    const currentUrl = this.router.url;
    const currentSymbol = currentUrl.startsWith('/symbols/')
      ? this.state.selectedSymbolLifecycle()?.symbol ?? null
      : null;
    await this.state.selectPortfolioRun(portfolioRunId);
    if (currentSymbol) {
      await this.state.loadSymbolLifecycle(currentSymbol);
      await this.router.navigate(['/symbols', currentSymbol], {
        queryParams: { portfolioRunId },
      });
      return;
    }
    await this.router.navigate(['/rankings'], {
      queryParams: { portfolioRunId },
      queryParamsHandling: 'merge',
    });
  }

  protected async onRankingRunClick(runId: number): Promise<void> {
    await this.state.selectRankingRun(runId);
    await this.router.navigate(['/rankings'], {
      queryParams: { runId },
      queryParamsHandling: 'merge',
    });
  }

  protected onRankingDateFilter(value: string): void {
    if (this.rankingDateFilterTimer !== null) {
      clearTimeout(this.rankingDateFilterTimer);
    }
    this.rankingDateFilterTimer = setTimeout(() => {
      void this.state.applyRankingDateFilter(value);
    }, 250);
  }

  protected onSymbolFilter(value: string): void {
    this.state.symbolFilter.set(value);
  }

  protected setLanguage(language: Language): void {
    this.i18n.setLanguage(language);
  }

  protected async loadMoreRankingDates(): Promise<void> {
    await this.state.loadMoreRankingDates();
  }

  protected onRankingDatesScroll(event: Event): void {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const remaining = target.scrollHeight - target.scrollTop - target.clientHeight;
    if (remaining < 48) {
      void this.state.loadMoreRankingDates();
    }
  }
}
