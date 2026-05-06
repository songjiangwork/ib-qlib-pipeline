import { Injectable, computed, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';
import { FrontendI18nService } from './frontend-i18n.service';

export type RunStatus = 'queued' | 'running' | 'succeeded' | 'failed';

export interface BackendConfig {
  project_root: string;
  db_path: string;
  timezone: string;
  default_workflow_base: string;
  run_script_path: string;
}

export interface ModelRef {
  id: number;
  key: string;
  name: string;
  model_class: string;
  workflow_base?: string | null;
}

export interface JobSummary {
  id: number;
  job_type: string;
  title: string;
  status: RunStatus;
  created_at: string;
  started_at?: string | null;
  finished_at?: string | null;
  error_text?: string | null;
}

export interface JobStep {
  id: number;
  job_id: number;
  step_order: number;
  step_name: string;
  status: RunStatus;
  command?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  log_output?: string | null;
  error_text?: string | null;
}

export interface JobDetail extends JobSummary {
  payload_json?: string | null;
  log_output?: string | null;
  steps: JobStep[];
}

export interface RunSummary {
  id: number;
  trigger_source: string;
  status: RunStatus;
  model_id?: number | null;
  model_key?: string | null;
  model_name?: string | null;
  model_class?: string | null;
  created_at: string;
  started_at?: string | null;
  finished_at?: string | null;
  signal_date?: string | null;
  row_count?: number | null;
  ranking_csv_path?: string | null;
}

export interface RankingDateItem {
  run_id: number;
  signal_date: string;
  row_count: number | null;
  model_id?: number | null;
  model_key?: string | null;
  model_name?: string | null;
}

export interface RankingDatePage {
  items: RankingDateItem[];
  total: number;
  has_more: boolean;
  next_offset: number;
}

export interface PerformanceMetric {
  date: string;
  price: number;
  change: number;
  return_pct: number;
  direction: 'up' | 'down';
}

export interface RecommendationRow {
  rank: number;
  symbol: string;
  score: number;
  percentile: number | null;
  entry_price: number | null;
  signal_date: string;
  performance?: Record<string, PerformanceMetric | null>;
}

export interface RecommendationSummary {
  count: number;
  avg_return_pct: number | null;
  win_rate_pct: number | null;
}

export interface RunRecommendationsResponse {
  run: RunSummary;
  horizons: number[];
  summary: Record<string, RecommendationSummary>;
  recommendations: RecommendationRow[];
}

export interface PortfolioRunSummary {
  id: number;
  name: string;
  strategy: string;
  model_id?: number | null;
  model_key?: string | null;
  model_name?: string | null;
  model_class?: string | null;
  workflow_base?: string | null;
  buy_top_n: number;
  hold_top_n: number;
  target_notional: number;
  start_signal_date: string;
  end_signal_date: string | null;
  created_at: string;
  lot_count: number;
  open_lot_count: number | null;
  closed_lot_count?: number | null;
  avg_hold_days?: number | null;
  avg_return_pct?: number | null;
  total_realized_pnl?: number | null;
  win_rate_pct?: number | null;
}

export interface PortfolioLot {
  id: number;
  portfolio_run_id: number;
  symbol: string;
  entry_run_id: number;
  entry_signal_date: string;
  entry_trade_date: string;
  entry_rank: number;
  entry_price_open: number;
  shares: number;
  target_notional: number;
  exit_run_id: number | null;
  exit_signal_date: string | null;
  exit_trade_date: string | null;
  exit_rank: number | null;
  exit_price_open: number | null;
  realized_pnl: number | null;
  realized_return_pct: number | null;
  status: 'open' | 'closed';
}

export interface PortfolioMark {
  id: number;
  portfolio_lot_id: number;
  trade_date: string;
  close_price: number;
  market_value: number;
  unrealized_pnl: number;
  unrealized_return_pct: number;
  is_in_top20: number;
  is_in_top10: number;
}

export interface PortfolioSymbolLifecycle {
  portfolio_run: PortfolioRunSummary;
  symbol: string;
  lots: Array<PortfolioLot & { marks: PortfolioMark[] }>;
}

export interface PriceBar {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number | null;
}

@Injectable({ providedIn: 'root' })
export class FrontendStateService {
  constructor(
    private readonly http: HttpClient,
    private readonly i18n: FrontendI18nService,
  ) {}

  readonly rankingDateFilter = signal('');
  readonly symbolFilter = signal('');
  readonly backendConfig = signal<BackendConfig | null>(null);
  readonly models = signal<ModelRef[]>([]);
  readonly recentRuns = signal<RunSummary[]>([]);
  readonly rankingDates = signal<RankingDateItem[]>([]);
  readonly rankingDatesTotal = signal(0);
  readonly rankingDatesHasMore = signal(false);
  readonly rankingDatesNextOffset = signal(0);
  readonly rankingDatesMode = signal<'browse' | 'search'>('browse');
  readonly rankingDatesLoading = signal(false);
  readonly selectedRankingRunId = signal<number | null>(null);
  readonly selectedRunData = signal<RunRecommendationsResponse | null>(null);
  readonly portfolioRuns = signal<PortfolioRunSummary[]>([]);
  readonly selectedPortfolioRunId = signal<number | null>(null);
  readonly portfolioLots = signal<PortfolioLot[]>([]);
  readonly compareSymbols = signal<string[]>([]);
  readonly selectedSymbolLifecycle = signal<PortfolioSymbolLifecycle | null>(null);
  readonly selectedSymbolPriceBars = signal<PriceBar[]>([]);
  readonly selectedPriceInterval = signal<'1d'>('1d');
  readonly jobs = signal<JobSummary[]>([]);
  readonly selectedJobId = signal<number | null>(null);
  readonly selectedJobDetail = signal<JobDetail | null>(null);
  readonly jobsLoading = signal(false);
  readonly jobActionError = signal<string | null>(null);
  readonly loadError = signal<string | null>(null);
  readonly detailError = signal<string | null>(null);
  readonly isLoading = signal(false);

  readonly selectedPortfolioRun = computed(() =>
    this.portfolioRuns().find((run) => run.id === this.selectedPortfolioRunId()) ?? null,
  );

  readonly symbolsInRun = computed(() => {
    const symbols = Array.from(new Set(this.portfolioLots().map((lot) => lot.symbol))).sort();
    const filter = this.symbolFilter().trim().toUpperCase();
    if (!filter) {
      return symbols;
    }
    return symbols.filter((symbol) => symbol.includes(filter));
  });

  async loadInitial(): Promise<void> {
    this.isLoading.set(true);
    this.loadError.set(null);
    try {
      const [config, runs, portfolioRuns, models] = await Promise.all([
        firstValueFrom(this.http.get<BackendConfig>('/api/config')),
        firstValueFrom(this.http.get<RunSummary[]>('/api/runs?limit=500')),
        firstValueFrom(this.http.get<PortfolioRunSummary[]>('/api/portfolio-runs')),
        firstValueFrom(this.http.get<ModelRef[]>('/api/models')),
      ]);

      this.backendConfig.set(config ?? null);
      this.recentRuns.set(runs ?? []);
      this.portfolioRuns.set(portfolioRuns ?? []);
      this.models.set(models ?? []);
      await this.loadRankingDates(true);

      if (!this.selectedRankingRunId() && runs && runs.length > 0) {
        await this.selectRankingRun(runs[0].id);
      }
      if (!this.selectedPortfolioRunId() && portfolioRuns && portfolioRuns.length > 0) {
        await this.selectPortfolioRun(portfolioRuns[0].id);
      }
    } catch {
      this.loadError.set(this.i18n.t('failedDashboard'));
    } finally {
      this.isLoading.set(false);
    }
  }

  async loadOperationsData(): Promise<void> {
    try {
      if (!this.models().length) {
        const models = await firstValueFrom(this.http.get<ModelRef[]>('/api/models'));
        this.models.set(models ?? []);
      }
      await this.loadJobs();
    } catch {
      this.jobActionError.set(this.i18n.t('failedJobs'));
    }
  }

  async loadJobs(): Promise<void> {
    this.jobsLoading.set(true);
    try {
      const jobs = await firstValueFrom(this.http.get<JobSummary[]>('/api/jobs', { params: { limit: 40 } }));
      this.jobs.set(jobs ?? []);
      const selected = this.selectedJobId();
      if (selected !== null) {
        await this.selectJob(selected);
      } else if ((jobs ?? []).length > 0) {
        await this.selectJob(jobs![0].id);
      }
    } catch {
      this.jobActionError.set(this.i18n.t('failedJobs'));
    } finally {
      this.jobsLoading.set(false);
    }
  }

  async selectJob(jobId: number): Promise<void> {
    this.selectedJobId.set(jobId);
    try {
      const detail = await firstValueFrom(this.http.get<JobDetail>(`/api/jobs/${jobId}`));
      this.selectedJobDetail.set(detail ?? null);
    } catch {
      this.selectedJobDetail.set(null);
      this.jobActionError.set(this.i18n.t('failedJobDetail'));
    }
  }

  async triggerRefreshData(clientId: number, startDate: string): Promise<void> {
    this.jobActionError.set(null);
    try {
      const job = await firstValueFrom(
        this.http.post<JobSummary>('/api/jobs/refresh-data', {
          client_id: clientId,
          start_date: startDate,
        }),
      );
      await this.loadJobs();
      if (job?.id) {
        await this.selectJob(job.id);
      }
    } catch (error: any) {
      this.jobActionError.set(error?.error?.detail || this.i18n.t('failedTriggerJob'));
    }
  }

  async triggerBackfillRanking(modelId: number, signalDate: string, clientId: number): Promise<void> {
    this.jobActionError.set(null);
    try {
      const job = await firstValueFrom(
        this.http.post<JobSummary>('/api/jobs/backfill-ranking', {
          model_id: modelId,
          signal_date: signalDate,
          client_id: clientId,
        }),
      );
      await this.loadJobs();
      if (job?.id) {
        await this.selectJob(job.id);
      }
    } catch (error: any) {
      this.jobActionError.set(error?.error?.detail || this.i18n.t('failedTriggerJob'));
    }
  }

  async triggerAppendPortfolio(portfolioRunId: number, modelId: number | null, endDate: string): Promise<void> {
    this.jobActionError.set(null);
    try {
      const job = await firstValueFrom(
        this.http.post<JobSummary>('/api/jobs/append-portfolio', {
          portfolio_run_id: portfolioRunId,
          model_id: modelId,
          end_date: endDate,
        }),
      );
      await this.loadJobs();
      if (job?.id) {
        await this.selectJob(job.id);
      }
    } catch (error: any) {
      this.jobActionError.set(error?.error?.detail || this.i18n.t('failedTriggerJob'));
    }
  }

  async triggerDailyClosePipeline(
    tradeDate: string,
    clientId: number,
    startDate: string,
    includePortfolio: boolean,
  ): Promise<void> {
    this.jobActionError.set(null);
    try {
      const job = await firstValueFrom(
        this.http.post<JobSummary>('/api/jobs/daily-close-pipeline', {
          trade_date: tradeDate,
          client_id: clientId,
          start_date: startDate,
          include_portfolio: includePortfolio,
        }),
      );
      await this.loadJobs();
      if (job?.id) {
        await this.selectJob(job.id);
      }
    } catch (error: any) {
      this.jobActionError.set(error?.error?.detail || this.i18n.t('failedTriggerJob'));
    }
  }

  async selectRankingRun(runId: number): Promise<void> {
    this.selectedRankingRunId.set(runId);
    this.detailError.set(null);
    try {
      const data = await firstValueFrom(
        this.http.get<RunRecommendationsResponse>(`/api/runs/${runId}/recommendations`, {
          params: { horizons: '1,5,10,21' },
        }),
      );
      this.selectedRunData.set(data ?? null);
    } catch {
      this.selectedRunData.set(null);
      this.detailError.set(this.i18n.t('failedRankingDetails'));
    }
  }

  async selectPortfolioRun(portfolioRunId: number): Promise<void> {
    this.selectedPortfolioRunId.set(portfolioRunId);
    this.selectedSymbolLifecycle.set(null);
    this.selectedSymbolPriceBars.set([]);
    this.detailError.set(null);
    try {
      const lots = await firstValueFrom(
        this.http.get<PortfolioLot[]>(`/api/portfolio-runs/${portfolioRunId}/lots`),
      );
      this.portfolioLots.set(lots ?? []);
      const symbols = new Set((lots ?? []).map((lot) => lot.symbol));
      this.compareSymbols.set(this.compareSymbols().filter((symbol) => symbols.has(symbol)));
      await this.loadRankingDates(true);
      const selectedModelId = this.selectedPortfolioRun()?.model_id ?? null;
      const selectedRunModelId = this.selectedRunData()?.run?.model_id ?? null;
      const currentRunStillVisible = this.rankingDates().some(
        (item) => item.run_id === this.selectedRankingRunId(),
      );
      if (!currentRunStillVisible || (selectedModelId !== null && selectedRunModelId !== selectedModelId)) {
        const firstRunId = this.rankingDates()[0]?.run_id ?? null;
        if (firstRunId !== null) {
          await this.selectRankingRun(firstRunId);
        } else {
          this.selectedRankingRunId.set(null);
          this.selectedRunData.set(null);
        }
      }
    } catch {
      this.portfolioLots.set([]);
      this.compareSymbols.set([]);
      this.detailError.set(this.i18n.t('failedPortfolioLots'));
    }
  }

  async loadSymbolLifecycle(symbol: string): Promise<void> {
    const portfolioRunId = this.selectedPortfolioRunId();
    if (portfolioRunId === null) {
      return;
    }
    this.detailError.set(null);
    try {
      const lifecycle = await firstValueFrom(
        this.http.get<PortfolioSymbolLifecycle>(
          `/api/portfolio-runs/${portfolioRunId}/symbols/${symbol}`,
        ),
      );
      this.selectedSymbolLifecycle.set(lifecycle ?? null);
      await this.loadSymbolPriceBars(symbol, lifecycle?.lots ?? []);
    } catch {
      this.selectedSymbolLifecycle.set(null);
      this.selectedSymbolPriceBars.set([]);
      this.detailError.set(this.i18n.t('failedSymbolLifecycle'));
    }
  }

  async loadRankingDates(reset: boolean, queryOverride?: string): Promise<void> {
    if (this.rankingDatesLoading()) {
      return;
    }
    const query = (queryOverride ?? this.rankingDateFilter()).trim();
    const mode = query ? 'search' : 'browse';
    const offset = reset ? 0 : this.rankingDatesNextOffset();
    this.rankingDatesLoading.set(true);
    try {
      const page = await firstValueFrom(
        this.http.get<RankingDatePage>('/api/ranking-dates', {
          params: {
            limit: 20,
            offset,
            ...(this.selectedPortfolioRun()?.model_id ? { model_id: this.selectedPortfolioRun()!.model_id! } : {}),
            ...(query ? { query } : {}),
          },
        }),
      );
      this.rankingDatesMode.set(mode);
      this.rankingDates.set(reset ? page.items : [...this.rankingDates(), ...page.items]);
      this.rankingDatesTotal.set(page.total);
      this.rankingDatesHasMore.set(page.has_more);
      this.rankingDatesNextOffset.set(page.next_offset);
    } catch {
      this.loadError.set(this.i18n.t('failedRankingDates'));
    } finally {
      this.rankingDatesLoading.set(false);
    }
  }

  async applyRankingDateFilter(value: string): Promise<void> {
    this.rankingDateFilter.set(value);
    await this.loadRankingDates(true, value);
  }

  async loadMoreRankingDates(): Promise<void> {
    if (!this.rankingDatesHasMore()) {
      return;
    }
    await this.loadRankingDates(false);
  }

  toggleCompareSymbol(symbol: string): void {
    const current = this.compareSymbols();
    if (current.includes(symbol)) {
      this.compareSymbols.set(current.filter((item) => item !== symbol));
      return;
    }
    if (current.length >= 5) {
      this.detailError.set(this.i18n.t('maxCompareReached'));
      return;
    }
    this.compareSymbols.set([...current, symbol]);
    this.detailError.set(null);
  }

  clearCompareSymbols(): void {
    this.compareSymbols.set([]);
  }

  isCompareSelected(symbol: string): boolean {
    return this.compareSymbols().includes(symbol);
  }

  isBoughtOnSignal(symbol: string, signalDate: string | null | undefined): boolean {
    if (!signalDate) {
      return false;
    }
    return this.portfolioLots().some(
      (lot) => lot.symbol === symbol && lot.entry_signal_date === signalDate,
    );
  }

  isHeldAfterSignal(symbol: string, signalDate: string | null | undefined): boolean {
    if (!signalDate) {
      return false;
    }
    return this.portfolioLots().some(
      (lot) =>
        lot.symbol === symbol &&
        lot.entry_signal_date <= signalDate &&
        (lot.exit_signal_date === null || lot.exit_signal_date > signalDate),
    );
  }

  async setPriceInterval(interval: '1d'): Promise<void> {
    this.selectedPriceInterval.set(interval);
    const symbol = this.selectedSymbolLifecycle()?.symbol;
    const lots = this.selectedSymbolLifecycle()?.lots ?? [];
    if (symbol) {
      await this.loadSymbolPriceBars(symbol, lots);
    }
  }

  private async loadSymbolPriceBars(
    symbol: string,
    lots: Array<PortfolioLot & { marks?: PortfolioMark[] }>,
  ): Promise<void> {
    if (!lots.length) {
      this.selectedSymbolPriceBars.set([]);
      return;
    }
    try {
      const prices = await firstValueFrom(
        this.http.get<PriceBar[]>(`/api/prices/${symbol}/bars`, {
          params: {
            interval: this.selectedPriceInterval(),
          },
        }),
      );
      this.selectedSymbolPriceBars.set(prices ?? []);
    } catch {
      this.selectedSymbolPriceBars.set([]);
    }
  }
}
