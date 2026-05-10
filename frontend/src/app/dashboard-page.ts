import { CommonModule } from '@angular/common';
import { Component, computed, inject } from '@angular/core';
import { RouterLink } from '@angular/router';

import { FrontendI18nService } from './frontend-i18n.service';
import { FrontendStateService } from './frontend-state.service';

@Component({
  standalone: true,
  selector: 'app-dashboard-page',
  imports: [CommonModule, RouterLink],
  templateUrl: './dashboard-page.html',
  styleUrl: './dashboard-page.css',
})
export class DashboardPage {
  protected readonly i18n = inject(FrontendI18nService);
  protected readonly state = inject(FrontendStateService);

  protected readonly summaryCards = computed(() => {
    const runs = this.state.recentRuns();
    const portfolios = this.state.portfolioRuns();
    const jobs = this.state.jobs();
    const runningJobs = jobs.filter((job) => job.status === 'queued' || job.status === 'running').length;
    const todaysSignals = new Set(runs.map((run) => run.signal_date).filter((value): value is string => !!value));
    return [
      { label: this.i18n.t('models'), value: String(this.state.models().length), detail: this.i18n.t('registeredModels') },
      { label: this.i18n.t('portfolioRuns'), value: String(portfolios.length), detail: this.i18n.t('trackedPortfolios') },
      { label: this.i18n.t('runningJobs'), value: String(runningJobs), detail: this.i18n.t('activeJobsNow') },
      { label: this.i18n.t('rankingDates'), value: String(todaysSignals.size), detail: this.i18n.t('loadedSignalDates') },
    ];
  });

  protected readonly modelStatusRows = computed(() => this.state.operationsSummary()?.models ?? []);
  protected readonly recentPortfolioRows = computed(() => this.state.portfolioRuns().slice(0, 6));
  protected readonly recentJobRows = computed(() => this.state.jobs().slice(0, 6));

  constructor() {
    void this.state.loadOperationsData();
  }
}
