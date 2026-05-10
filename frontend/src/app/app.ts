import { CommonModule } from '@angular/common';
import { Component, inject, signal } from '@angular/core';
import { RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';

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
  protected readonly navOpen = signal(false);

  constructor() {
    void this.state.loadInitial();
  }

  protected setLanguage(language: Language): void {
    this.i18n.setLanguage(language);
  }

  protected toggleNav(): void {
    this.navOpen.update((value) => !value);
  }

  protected closeNav(): void {
    this.navOpen.set(false);
  }
}
