import { CommonModule, DatePipe, DecimalPipe } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { Component, computed, inject, signal } from '@angular/core';
import { firstValueFrom } from 'rxjs';

type RunStatus = 'queued' | 'running' | 'succeeded' | 'failed';

interface BackendConfig {
  project_root: string;
  db_path: string;
  timezone: string;
  default_workflow_base: string;
  run_script_path: string;
}

interface RunSummary {
  id: number;
  schedule_id: number | null;
  schedule_name?: string | null;
  trigger_source: string;
  status: RunStatus;
  created_at: string;
  started_at?: string | null;
  finished_at?: string | null;
  signal_date?: string | null;
  row_count?: number | null;
  ranking_csv_path?: string | null;
  error_text?: string | null;
}

interface PerformanceMetric {
  date: string;
  price: number;
  change: number;
  return_pct: number;
  direction: 'up' | 'down';
}

interface RecommendationRow {
  rank: number;
  symbol: string;
  score: number;
  percentile: number | null;
  entry_price: number | null;
  signal_date: string;
  performance?: Record<string, PerformanceMetric | null>;
}

interface RecommendationSummary {
  count: number;
  avg_return_pct: number | null;
  win_rate_pct: number | null;
}

interface RunRecommendationsResponse {
  run: RunSummary;
  horizons: number[];
  summary: Record<string, RecommendationSummary>;
  recommendations: RecommendationRow[];
}

@Component({
  selector: 'app-root',
  imports: [CommonModule, DatePipe, DecimalPipe],
  templateUrl: './app.html',
  styleUrl: './app.css',
})
export class App {
  private readonly http = inject(HttpClient);

  protected readonly title = 'IB Qlib Ranking Console';
  protected readonly backendConfig = signal<BackendConfig | null>(null);
  protected readonly runs = signal<RunSummary[]>([]);
  protected readonly selectedRunId = signal<number | null>(null);
  protected readonly selectedRunData = signal<RunRecommendationsResponse | null>(null);
  protected readonly isLoadingRuns = signal(false);
  protected readonly isLoadingDetails = signal(false);
  protected readonly loadError = signal<string | null>(null);
  protected readonly detailError = signal<string | null>(null);
  protected readonly horizons = signal('1,5,10,21');
  protected readonly selectedRun = computed(() =>
    this.runs().find((run) => run.id === this.selectedRunId()) ?? null,
  );

  constructor() {
    void this.loadDashboard();
  }

  protected async refreshRuns(): Promise<void> {
    await this.loadRuns();
  }

  protected async selectRun(runId: number): Promise<void> {
    this.selectedRunId.set(runId);
    this.detailError.set(null);
    this.isLoadingDetails.set(true);
    try {
      const data = await firstValueFrom(
        this.http.get<RunRecommendationsResponse>(`/api/runs/${runId}/recommendations`, {
          params: { horizons: this.horizons() },
        }),
      );
      this.selectedRunData.set(data ?? null);
    } catch {
      this.selectedRunData.set(null);
      this.detailError.set('Failed to load recommendation details from backend.');
    } finally {
      this.isLoadingDetails.set(false);
    }
  }

  protected async applyHorizons(rawValue: string): Promise<void> {
    this.horizons.set(rawValue);
    const runId = this.selectedRunId();
    if (runId !== null) {
      await this.selectRun(runId);
    }
  }

  protected objectKeys(value: Record<string, RecommendationSummary> | undefined): string[] {
    return value ? Object.keys(value) : [];
  }

  protected performanceClass(metric: PerformanceMetric | null | undefined): string {
    if (!metric) {
      return '';
    }
    return metric.return_pct >= 0 ? 'up' : 'down';
  }

  private async loadDashboard(): Promise<void> {
    await Promise.all([this.loadConfig(), this.loadRuns()]);
  }

  private async loadConfig(): Promise<void> {
    try {
      const config = await firstValueFrom(this.http.get<BackendConfig>('/api/config'));
      this.backendConfig.set(config ?? null);
    } catch {
      this.backendConfig.set(null);
      this.loadError.set('Frontend is up, but backend /api/config is not reachable.');
    }
  }

  private async loadRuns(): Promise<void> {
    this.isLoadingRuns.set(true);
    this.loadError.set(null);
    try {
      const runs = await firstValueFrom(this.http.get<RunSummary[]>('/api/runs?limit=30'));
      this.runs.set(runs ?? []);
      const currentId = this.selectedRunId();
      const nextId = currentId ?? (runs && runs.length > 0 ? runs[0].id : null);
      if (nextId !== null) {
        await this.selectRun(nextId);
      } else {
        this.selectedRunData.set(null);
      }
    } catch {
      this.runs.set([]);
      this.selectedRunData.set(null);
      this.loadError.set('Failed to load run history. Start the backend and check /api/runs.');
    } finally {
      this.isLoadingRuns.set(false);
    }
  }
}
