import { CommonModule, DecimalPipe } from '@angular/common';
import { Component, ElementRef, ViewChild, computed, effect, inject, signal } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';
import { ActivatedRoute, Router } from '@angular/router';
import {
  CandlestickSeries,
  ColorType,
  IChartApi,
  LineSeries,
  PriceScaleMode,
  Time,
  createChart,
  createSeriesMarkers,
} from 'lightweight-charts';
import { map } from 'rxjs/operators';

import { FrontendI18nService } from './frontend-i18n.service';
import { FrontendStateService, PortfolioLot, PortfolioMark, PriceBar } from './frontend-state.service';

type ChartRange = '1m' | '3m' | '6m' | '1y' | 'all';
type TradeEvent = {
  lotId: number;
  buyDate: string;
  sellDate: string | null;
  buyPrice: number;
  sellPrice: number | null;
  pnlPct: number | null;
};

@Component({
  standalone: true,
  selector: 'app-symbol-detail-page',
  imports: [CommonModule, DecimalPipe],
  templateUrl: './symbol-detail-page.html',
  styleUrl: './symbol-detail-page.css',
})
export class SymbolDetailPage {
  @ViewChild('chartHost')
  set chartHost(value: ElementRef<HTMLDivElement> | undefined) {
    this.chartHostRef = value?.nativeElement ?? null;
    this.syncChart();
  }

  protected readonly i18n = inject(FrontendI18nService);
  protected readonly state = inject(FrontendStateService);
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly routeSymbol = toSignal(
    this.route.paramMap.pipe(map((params) => params.get('symbol'))),
    { initialValue: this.route.snapshot.paramMap.get('symbol') },
  );
  private readonly queryUniverseId = toSignal(
    this.route.queryParamMap.pipe(map((params) => params.get('u'))),
    { initialValue: this.route.snapshot.queryParamMap.get('u') },
  );
  private readonly queryPortfolioRunId = toSignal(
    this.route.queryParamMap.pipe(map((params) => params.get('p'))),
    { initialValue: this.route.snapshot.queryParamMap.get('p') },
  );

  private chartHostRef: HTMLDivElement | null = null;
  private chart: IChartApi | null = null;
  private candleSeries: any = null;
  private percentSeries: any = null;
  private markerPlugin: any = null;
  private resizeObserver: ResizeObserver | null = null;
  private readonly eventSortKey = signal<'lotId' | 'buyDate' | 'sellDate' | 'buyPrice' | 'sellPrice' | 'pnlPct'>('buyDate');
  private readonly eventSortDirection = signal<'asc' | 'desc'>('asc');
  private readonly markSortKey = signal<'trade_date' | 'close_price' | 'market_value' | 'unrealized_pnl' | 'unrealized_return_pct' | 'is_in_top20' | 'is_in_top10'>('trade_date');
  private readonly markSortDirection = signal<'asc' | 'desc'>('asc');
  private applyingQueryState = false;
  private lastAppliedQueryKey: string | null = null;

  protected readonly lifecycle = computed(() => this.state.selectedSymbolLifecycle());
  protected readonly priceBars = computed(() => this.state.selectedSymbolPriceBars());
  protected readonly selectedRange = signal<ChartRange>('6m');
  protected readonly displayPortfolioRun = computed(
    () => this.lifecycle()?.portfolio_run ?? this.state.selectedPortfolioRun(),
  );

  protected readonly headlineStats = computed(() => {
    const lots = this.lifecycle()?.lots ?? [];
    const closed = lots.filter((lot) => lot.status === 'closed');
    const open = lots.filter((lot) => lot.status === 'open');
    const realizedPnl = closed.reduce((sum, lot) => sum + (lot.realized_pnl ?? 0), 0);
    const latestMarks = open
      .map((lot) => this.latestMark(lot.marks))
      .filter((mark): mark is PortfolioMark => mark !== null);
    const latestUnrealized = latestMarks.reduce((sum, mark) => sum + mark.unrealized_pnl, 0);
    const holdDays = closed
      .map((lot) => this.holdDays(lot))
      .filter((days): days is number => days !== null);
    const returnPcts = closed
      .map((lot) => lot.realized_return_pct)
      .filter((value): value is number => value !== null);
    const winCount = returnPcts.filter((value) => value > 0).length;
    return {
      lotCount: lots.length,
      closedCount: closed.length,
      openCount: open.length,
      realizedPnl,
      latestUnrealized,
      avgHoldDays: holdDays.length ? holdDays.reduce((sum, value) => sum + value, 0) / holdDays.length : null,
      winRatePct: returnPcts.length ? (winCount / returnPcts.length) * 100 : null,
      avgReturnPct: returnPcts.length ? returnPcts.reduce((sum, value) => sum + value, 0) / returnPcts.length : null,
    };
  });

  protected readonly chartSummary = computed(() => {
    const bars = this.priceBars();
    if (!bars.length) {
      return null;
    }
    const closes = bars.map((bar) => bar.close);
    return {
      firstTime: bars[0].time,
      lastTime: bars[bars.length - 1].time,
      lastClose: bars[bars.length - 1].close,
      minPrice: Math.min(...closes),
      maxPrice: Math.max(...closes),
    };
  });
  protected readonly currentInstrument = computed(() => {
    const symbol = this.lifecycle()?.symbol ?? null;
    if (!symbol) {
      return null;
    }
    return this.state.instrumentMap()[symbol] ?? null;
  });
  protected readonly tradeEvents = computed<TradeEvent[]>(() => {
    const lots = this.lifecycle()?.lots ?? [];
    return lots
      .map((lot) => ({
        lotId: lot.id,
        buyDate: lot.entry_trade_date,
        sellDate: lot.exit_trade_date,
        buyPrice: lot.entry_price_open,
        sellPrice: lot.exit_price_open,
        pnlPct: lot.realized_return_pct,
      }))
      .sort((a, b) => this.compareEvents(a, b));
  });

  constructor() {
    effect(() => {
      const initialized = this.state.initialized();
      const universe = this.queryUniverseId();
      const portfolio = this.queryPortfolioRunId();
      const symbol = this.routeSymbol();
      if (!initialized) {
        return;
      }
      const key = `${universe ?? ''}|${portfolio ?? ''}|${symbol ?? ''}`;
      if (this.applyingQueryState || this.lastAppliedQueryKey === key) {
        return;
      }
      void this.applyQueryState(universe, portfolio, symbol, key);
    });

    effect(() => {
      const symbol = this.routeSymbol();
      if (symbol) {
        void this.state.loadSymbolLifecycle(symbol);
      }
    });

    effect(() => {
      this.priceBars();
      this.lifecycle();
      this.selectedRange();
      this.syncChart();
    });

    effect(() => {
      if (!this.state.initialized() || this.applyingQueryState) {
        return;
      }
      const currentSymbol = this.routeSymbol();
      if (!currentSymbol) {
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
      void this.router.navigate(['/symbols', currentSymbol], {
        queryParams: desired,
        replaceUrl: true,
      });
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

  protected toggleEventSort(key: 'lotId' | 'buyDate' | 'sellDate' | 'buyPrice' | 'sellPrice' | 'pnlPct'): void {
    if (this.eventSortKey() === key) {
      this.eventSortDirection.set(this.eventSortDirection() === 'asc' ? 'desc' : 'asc');
      return;
    }
    this.eventSortKey.set(key);
    this.eventSortDirection.set(key === 'buyDate' || key === 'sellDate' ? 'asc' : 'desc');
  }

  protected toggleMarkSort(
    key: 'trade_date' | 'close_price' | 'market_value' | 'unrealized_pnl' | 'unrealized_return_pct' | 'is_in_top20' | 'is_in_top10',
  ): void {
    if (this.markSortKey() === key) {
      this.markSortDirection.set(this.markSortDirection() === 'asc' ? 'desc' : 'asc');
      return;
    }
    this.markSortKey.set(key);
    this.markSortDirection.set(key === 'trade_date' ? 'asc' : 'desc');
  }

  protected eventSortMarker(key: 'lotId' | 'buyDate' | 'sellDate' | 'buyPrice' | 'sellPrice' | 'pnlPct'): string {
    return this.sortMarker(this.eventSortKey(), this.eventSortDirection(), key);
  }

  protected markSortMarker(
    key: 'trade_date' | 'close_price' | 'market_value' | 'unrealized_pnl' | 'unrealized_return_pct' | 'is_in_top20' | 'is_in_top10',
  ): string {
    return this.sortMarker(this.markSortKey(), this.markSortDirection(), key);
  }

  protected sortedMarks(marks: PortfolioMark[]): PortfolioMark[] {
    return [...marks].sort((a, b) => this.compareMarks(a, b));
  }

  private holdDays(lot: PortfolioLot): number | null {
    if (!lot.exit_trade_date) {
      return null;
    }
    const start = new Date(lot.entry_trade_date);
    const end = new Date(lot.exit_trade_date);
    return Math.round((end.getTime() - start.getTime()) / 86400000);
  }

  protected modelDisplay(): string {
    const run = this.displayPortfolioRun();
    if (!run) {
      return 'N/A';
    }
    if (run.model_name) {
      return `${run.model_name} (${run.model_key || 'n/a'})`;
    }
    return 'N/A';
  }

  protected symbolDisplay(): string {
    const symbol = this.lifecycle()?.symbol ?? '';
    const displayName = this.state.displayNameForSymbol(symbol);
    return displayName ? `${displayName} · ${symbol}` : symbol || 'N/A';
  }

  protected async setInterval(interval: '1d'): Promise<void> {
    await this.state.setPriceInterval(interval);
  }

  protected async onPortfolioRunChange(value: string): Promise<void> {
    const portfolioRunId = Number(value);
    if (!Number.isFinite(portfolioRunId) || portfolioRunId <= 0) {
      return;
    }
    await this.state.selectPortfolioRun(portfolioRunId);
    const currentSymbol = this.routeSymbol();
    const nextSymbol =
      currentSymbol && this.state.symbolsInRun().includes(currentSymbol)
        ? currentSymbol
        : this.state.symbolsInRun()[0] ?? null;
    if (nextSymbol) {
      await this.state.loadSymbolLifecycle(nextSymbol);
      await this.router.navigate(['/symbols', nextSymbol], {
        queryParams: {
          u: this.state.selectedUniverseId() ?? null,
          p: this.state.selectedPortfolioRunId() ?? null,
        },
      });
    }
  }

  protected async onSymbolChange(value: string): Promise<void> {
    const symbol = value.trim().toUpperCase();
    if (!symbol) {
      return;
    }
    await this.state.loadSymbolLifecycle(symbol);
    await this.router.navigate(['/symbols', symbol], {
      queryParams: {
        u: this.state.selectedUniverseId() ?? null,
        p: this.state.selectedPortfolioRunId() ?? null,
      },
    });
  }

  protected setRange(range: ChartRange): void {
    this.selectedRange.set(range);
    this.applyVisibleRange();
  }

  private syncChart(): void {
    if (!this.chartHostRef) {
      return;
    }
    const bars = this.priceBars();
    if (!bars.length) {
      this.destroyChart();
      return;
    }
    if (!this.chart) {
      this.createChart();
    }
    if (!this.chart || !this.candleSeries || !this.percentSeries) {
      return;
    }

    this.candleSeries.setData(
      bars.map((bar) => ({
        time: bar.time as Time,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
      })),
    );
    this.percentSeries.setData(
      bars.map((bar) => ({
        time: bar.time as Time,
        value: bar.close,
      })),
    );
    this.applyMarkers(bars, this.lifecycle()?.lots ?? []);
    this.applyVisibleRange();
  }

  private createChart(): void {
    if (!this.chartHostRef) {
      return;
    }
    this.chart = createChart(this.chartHostRef, {
      autoSize: true,
      height: 420,
      layout: {
        background: { type: ColorType.Solid, color: '#0d1727' },
        textColor: '#d7e3f8',
      },
      grid: {
        vertLines: { color: 'rgba(151, 178, 219, 0.12)' },
        horzLines: { color: 'rgba(151, 178, 219, 0.12)' },
      },
      rightPriceScale: {
        visible: true,
        borderColor: 'rgba(151, 178, 219, 0.2)',
        mode: PriceScaleMode.Normal,
      },
      leftPriceScale: {
        visible: true,
        borderColor: 'rgba(151, 178, 219, 0.2)',
        mode: PriceScaleMode.Percentage,
      },
      timeScale: {
        borderColor: 'rgba(151, 178, 219, 0.2)',
        timeVisible: true,
      },
      crosshair: {
        vertLine: { color: 'rgba(81, 203, 255, 0.35)' },
        horzLine: { color: 'rgba(81, 203, 255, 0.35)' },
      },
      localization: {
        priceFormatter: (price: number) => price.toFixed(2),
      },
    });

    this.candleSeries = this.chart.addSeries(CandlestickSeries, {
      upColor: '#0c7a5a',
      downColor: '#a63d40',
      wickUpColor: '#0c7a5a',
      wickDownColor: '#a63d40',
      borderUpColor: '#0c7a5a',
      borderDownColor: '#a63d40',
      priceScaleId: 'right',
    });

    this.percentSeries = this.chart.addSeries(LineSeries, {
      color: 'rgba(12, 122, 90, 0)',
      lineWidth: 1,
      priceScaleId: 'left',
      lastValueVisible: false,
      priceLineVisible: false,
      crosshairMarkerVisible: false,
    });

    this.resizeObserver = new ResizeObserver(() => {
      this.chart?.timeScale().fitContent();
      this.applyVisibleRange();
    });
    this.resizeObserver.observe(this.chartHostRef);
  }

  private applyMarkers(bars: PriceBar[], lots: PortfolioLot[]): void {
    if (!this.candleSeries) {
      return;
    }
    const barTimes = new Set(bars.map((bar) => bar.time));
    const markers = lots.flatMap((lot) => {
      const items: Array<{
        time: Time;
        position: 'belowBar' | 'aboveBar';
        color: string;
        shape: 'arrowUp' | 'arrowDown';
        text: string;
      }> = [];
      if (barTimes.has(lot.entry_trade_date)) {
        items.push({
          time: lot.entry_trade_date as Time,
          position: 'belowBar',
          color: '#0c7a5a',
          shape: 'arrowUp',
          text: `BUY ${lot.entry_trade_date} @ ${lot.entry_price_open.toFixed(2)}`,
        });
      }
      if (lot.exit_trade_date && barTimes.has(lot.exit_trade_date)) {
        items.push({
          time: lot.exit_trade_date as Time,
          position: 'aboveBar',
          color: '#a63d40',
          shape: 'arrowDown',
          text: `SELL ${lot.exit_trade_date} @ ${lot.exit_price_open?.toFixed(2) ?? 'N/A'}`,
        });
      }
      return items;
    });
    if (!this.markerPlugin) {
      this.markerPlugin = createSeriesMarkers(this.candleSeries, markers);
      return;
    }
    this.markerPlugin.setMarkers(markers);
  }

  private applyVisibleRange(): void {
    if (!this.chart) {
      return;
    }
    const bars = this.priceBars();
    if (!bars.length) {
      return;
    }
    if (this.selectedRange() === 'all') {
      this.chart.timeScale().fitContent();
      return;
    }
    const sizeMap: Record<Exclude<ChartRange, 'all'>, number> = {
      '1m': 22,
      '3m': 66,
      '6m': 132,
      '1y': 252,
    };
    const count = sizeMap[this.selectedRange() as Exclude<ChartRange, 'all'>];
    const fromIndex = Math.max(0, bars.length - count);
    this.chart.timeScale().setVisibleRange({
      from: bars[fromIndex].time as Time,
      to: bars[bars.length - 1].time as Time,
    });
  }

  private destroyChart(): void {
    this.resizeObserver?.disconnect();
    this.resizeObserver = null;
    this.chart?.remove();
    this.chart = null;
    this.candleSeries = null;
    this.percentSeries = null;
    this.markerPlugin = null;
  }

  private compareEvents(a: TradeEvent, b: TradeEvent): number {
    const key = this.eventSortKey();
    const dir = this.eventSortDirection() === 'asc' ? 1 : -1;
    return this.compareValues(a[key], b[key]) * dir;
  }

  private compareMarks(a: PortfolioMark, b: PortfolioMark): number {
    const key = this.markSortKey();
    const dir = this.markSortDirection() === 'asc' ? 1 : -1;
    return this.compareValues((a as any)[key], (b as any)[key]) * dir;
  }

  private sortMarker(currentKey: string, currentDirection: 'asc' | 'desc', key: string): string {
    if (currentKey !== key) {
      return '';
    }
    return currentDirection === 'asc' ? ' ▲' : ' ▼';
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
    symbolParam: string | null,
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

      const currentSymbol =
        symbolParam && this.state.symbolsInRun().includes(symbolParam)
          ? symbolParam
          : (this.state.symbolsInRun()[0] ?? null);
      if (currentSymbol) {
        await this.state.loadSymbolLifecycle(currentSymbol);
        if (currentSymbol !== symbolParam) {
          await this.router.navigate(['/symbols', currentSymbol], {
            queryParams: {
              u: this.state.selectedUniverseId() ?? null,
              p: this.state.selectedPortfolioRunId() ?? null,
            },
            replaceUrl: true,
          });
        }
      }
      this.lastAppliedQueryKey = key;
    } finally {
      this.applyingQueryState = false;
    }
  }
}
