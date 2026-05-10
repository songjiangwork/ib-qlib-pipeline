import { Routes } from '@angular/router';
import { DailyRankingsPage } from './daily-rankings-page';
import { ComparePage } from './compare-page';
import { SymbolDetailPage } from './symbol-detail-page';
import { OperationsPage } from './operations-page';

export const routes: Routes = [
  { path: '', pathMatch: 'full', redirectTo: 'rankings' },
  { path: 'operations', component: OperationsPage },
  { path: 'rankings', component: DailyRankingsPage },
  { path: 'compare', component: ComparePage },
  { path: 'symbols/:symbol', component: SymbolDetailPage },
];
