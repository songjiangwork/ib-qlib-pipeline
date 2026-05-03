import { Routes } from '@angular/router';
import { DailyRankingsPage } from './daily-rankings-page';
import { SymbolDetailPage } from './symbol-detail-page';

export const routes: Routes = [
  { path: '', pathMatch: 'full', redirectTo: 'rankings' },
  { path: 'rankings', component: DailyRankingsPage },
  { path: 'symbols/:symbol', component: SymbolDetailPage },
];
