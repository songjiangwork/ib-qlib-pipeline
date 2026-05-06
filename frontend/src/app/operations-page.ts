import { CommonModule } from '@angular/common';
import { Component, OnDestroy, OnInit, computed, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';

import { FrontendI18nService } from './frontend-i18n.service';
import { FrontendStateService } from './frontend-state.service';

function todayIso(): string {
  return new Date().toISOString().slice(0, 10);
}

function daysAgoIso(days: number): string {
  const value = new Date();
  value.setDate(value.getDate() - days);
  return value.toISOString().slice(0, 10);
}

@Component({
  standalone: true,
  selector: 'app-operations-page',
  imports: [CommonModule, FormsModule],
  templateUrl: './operations-page.html',
  styleUrl: './operations-page.css',
})
export class OperationsPage implements OnInit, OnDestroy {
  protected readonly i18n = inject(FrontendI18nService);
  protected readonly state = inject(FrontendStateService);
  protected readonly refreshClientId = signal(151);
  protected readonly refreshStartDate = signal(daysAgoIso(7));
  protected readonly pipelineTradeDate = signal(todayIso());
  protected readonly pipelineClientId = signal(151);
  protected readonly pipelineStartDate = signal(daysAgoIso(7));
  protected readonly pipelineIncludePortfolio = signal(true);
  protected readonly backfillTradeDate = signal(todayIso());
  protected readonly backfillClientId = signal(151);
  protected readonly backfillModelId = signal<number | null>(null);
  protected readonly appendPortfolioRunId = signal<number | null>(null);
  protected readonly appendEndDate = signal(daysAgoIso(1));
  private pollTimer: ReturnType<typeof setInterval> | null = null;

  protected readonly runningJobs = computed(() =>
    this.state.jobs().filter((job) => job.status === 'queued' || job.status === 'running'),
  );
  protected readonly operationsModels = computed(() => this.state.operationsSummary()?.models ?? []);

  async ngOnInit(): Promise<void> {
    await this.state.loadOperationsData();
    if (this.state.models().length && this.backfillModelId() === null) {
      this.backfillModelId.set(this.state.models()[0].id);
    }
    if (this.state.portfolioRuns().length && this.appendPortfolioRunId() === null) {
      this.appendPortfolioRunId.set(this.state.portfolioRuns()[0].id);
    }
    this.pollTimer = setInterval(() => {
      void this.state.loadJobs();
    }, 5000);
  }

  ngOnDestroy(): void {
    if (this.pollTimer !== null) {
      clearInterval(this.pollTimer);
    }
  }

  protected async runRefreshData(): Promise<void> {
    await this.state.triggerRefreshData(this.refreshClientId(), this.refreshStartDate());
  }

  protected async runBackfill(): Promise<void> {
    const modelId = this.backfillModelId();
    if (modelId === null) {
      return;
    }
    await this.state.triggerBackfillRanking(modelId, this.backfillTradeDate(), this.backfillClientId());
  }

  protected async runAppendPortfolio(): Promise<void> {
    const portfolioRunId = this.appendPortfolioRunId();
    if (portfolioRunId === null) {
      return;
    }
    const run = this.state.portfolioRuns().find((item) => item.id === portfolioRunId) ?? null;
    await this.state.triggerAppendPortfolio(portfolioRunId, run?.model_id ?? null, this.appendEndDate());
  }

  protected async runDailyClosePipeline(): Promise<void> {
    await this.state.loadOperationsSummary(this.pipelineTradeDate());
    await this.state.triggerDailyClosePipeline(
      this.pipelineTradeDate(),
      this.pipelineClientId(),
      this.pipelineStartDate(),
      this.pipelineIncludePortfolio(),
    );
  }

  protected async openJob(jobId: number): Promise<void> {
    await this.state.selectJob(jobId);
  }

  protected async reloadSummary(): Promise<void> {
    await this.state.loadOperationsSummary(this.pipelineTradeDate());
  }
}
