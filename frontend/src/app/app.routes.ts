import { Routes } from '@angular/router';
import { DashboardPage } from './dashboard-page';
import { DailyRankingsPage } from './daily-rankings-page';
import { ComparePage } from './compare-page';
import { SymbolDetailPage } from './symbol-detail-page';
import { OperationsPage } from './operations-page';
import { PortfolioPage } from './portfolio-page';

export const routes: Routes = [
  { path: '', pathMatch: 'full', redirectTo: 'dashboard' },
  { path: 'dashboard', component: DashboardPage },
  { path: 'operations', component: OperationsPage },
  { path: 'portfolios', component: PortfolioPage },
  { path: 'rankings', component: DailyRankingsPage },
  { path: 'compare', component: ComparePage },
  { path: 'symbols/:symbol', component: SymbolDetailPage },
];
