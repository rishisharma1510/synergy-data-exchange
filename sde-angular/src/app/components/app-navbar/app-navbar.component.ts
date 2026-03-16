import { Component, inject, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule, Router } from '@angular/router';
import { AuthService } from '../../services/auth.service';
import { TenantService, Tenant } from '../../services/tenant.service';

const SECTIONS = [
  {
    label: 'WORKFLOWS',
    items: [
      { to: '/', label: 'Dashboard', icon: 'layout-dashboard' },
      { to: '/extraction', label: 'Data Extraction', icon: 'database' },
      { to: '/ingestion', label: 'Data Ingestion', icon: 'upload' },
      { to: '/activity', label: 'Activity', icon: 'activity' },
    ],
  },
  {
    label: 'MANAGEMENT',
    items: [
      { to: '#', label: 'Synergy Drive', icon: 'folder-open' },
      { to: '#', label: 'Analytics', icon: 'bar-chart-3' },
    ],
  },
  {
    label: 'SUPPORT',
    items: [
      { to: '#', label: 'Admin Console', icon: 'settings' },
      { to: '#', label: 'API Documentation', icon: 'help-circle' },
    ],
  },
];

@Component({
  selector: 'app-navbar',
  standalone: true,
  imports: [CommonModule, RouterModule],
  template: `
    <!-- Top bar -->
    <header class="h-12 bg-sidebar border-b border-sidebar-border flex items-center justify-between px-4 shrink-0 relative z-50">
      <div class="flex items-center gap-3">
        <button (click)="togglePanel()" class="text-muted-foreground hover:text-foreground transition-colors">
          @if (panelOpen()) {
            <!-- X icon -->
            <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 6 6 18"/><path d="m6 6 12 12"/></svg>
          } @else {
            <!-- Menu icon -->
            <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="4" x2="20" y1="12" y2="12"/><line x1="4" x2="20" y1="6" y2="6"/><line x1="4" x2="20" y1="18" y2="18"/></svg>
          }
        </button>
        <span class="text-sm font-semibold text-foreground tracking-tight">Verisk Synergy Studio</span>
      </div>
      <div class="flex items-center gap-3">
        <!-- Tenant dropdown -->
        <div class="relative">
          <button
            (click)="toggleTenantDropdown()"
            class="flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground transition-colors px-2 py-1 rounded-md border border-border bg-background"
          >
            <span class="font-medium">{{ tenantService.currentTenant().name }}</span>
            <!-- ChevronDown -->
            <svg class="h-3.5 w-3.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"/></svg>
          </button>
          @if (tenantDropdownOpen()) {
            <div class="absolute right-0 mt-1 w-48 rounded-md border border-border bg-popover shadow-lg z-50">
              @for (t of tenantService.tenants; track t.id) {
                <button
                  (click)="selectTenant(t)"
                  class="flex w-full items-center px-3 py-2 text-sm hover:bg-accent transition-colors"
                  [class.font-medium]="tenantService.currentTenant().id === t.id"
                  [class.bg-accent]="tenantService.currentTenant().id === t.id"
                >
                  {{ t.name }}
                </button>
              }
            </div>
          }
        </div>
        <!-- User/Logout button -->
        <button (click)="authService.logout()" class="text-muted-foreground hover:text-foreground transition-colors">
          <!-- User icon -->
          <svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="8" r="5"/><path d="M20 21a8 8 0 0 0-16 0"/></svg>
        </button>
      </div>
    </header>

    <!-- Slide-out side panel + overlay -->
    @if (panelOpen()) {
      <div class="fixed inset-0 top-12 z-40 flex">
        <!-- Panel -->
        <nav class="w-64 bg-sidebar border-r border-sidebar-border h-full overflow-y-auto py-4 animate-slide-in-from-left">
          @for (section of sections; track section.label) {
            <div class="mb-4">
              <p class="px-5 mb-2 text-[10px] tracking-[0.15em] uppercase text-muted-foreground font-semibold">
                {{ section.label }}
              </p>
              @for (item of section.items; track item.label) {
                <a
                  [routerLink]="item.to === '#' ? null : item.to"
                  (click)="item.to !== '#' && closePanel()"
                  class="flex items-center gap-3 px-5 py-2 text-sm transition-colors cursor-pointer"
                  [class]="getLinkClass(item.to)"
                >
                  @switch (item.icon) {
                    @case ('layout-dashboard') {
                      <svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="7" height="9" x="3" y="3" rx="1"/><rect width="7" height="5" x="14" y="3" rx="1"/><rect width="7" height="9" x="14" y="12" rx="1"/><rect width="7" height="5" x="3" y="16" rx="1"/></svg>
                    }
                    @case ('database') {
                      <svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5V19A9 3 0 0 0 21 19V5"/><path d="M3 12A9 3 0 0 0 21 12"/></svg>
                    }
                    @case ('upload') {
                      <svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" x2="12" y1="3" y2="15"/></svg>
                    }
                    @case ('activity') {
                      <svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 12h-2.48a2 2 0 0 0-1.93 1.46l-2.35 8.36a.25.25 0 0 1-.48 0L9.24 2.18a.25.25 0 0 0-.48 0l-2.35 8.36A2 2 0 0 1 4.49 12H2"/></svg>
                    }
                    @case ('folder-open') {
                      <svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m6 14 1.5-2.9A2 2 0 0 1 9.24 10H20a2 2 0 0 1 1.94 2.5l-1.54 6a2 2 0 0 1-1.95 1.5H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h3.9a2 2 0 0 1 1.69.9l.81 1.2a2 2 0 0 0 1.67.9H18a2 2 0 0 1 2 2v2"/></svg>
                    }
                    @case ('bar-chart-3') {
                      <svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18"/><path d="M18 17V9"/><path d="M13 17V5"/><path d="M8 17v-3"/></svg>
                    }
                    @case ('settings') {
                      <svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z"/><circle cx="12" cy="12" r="3"/></svg>
                    }
                    @case ('help-circle') {
                      <svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/><path d="M12 17h.01"/></svg>
                    }
                  }
                  {{ item.label }}
                </a>
              }
            </div>
          }
        </nav>
        <!-- Backdrop -->
        <div
          class="flex-1 bg-background/60 backdrop-blur-sm"
          (click)="closePanel()"
        ></div>
      </div>
    }
  `,
})
export class AppNavbarComponent {
  readonly authService = inject(AuthService);
  readonly tenantService = inject(TenantService);
  private readonly router = inject(Router);

  panelOpen = signal(false);
  tenantDropdownOpen = signal(false);
  sections = SECTIONS;

  togglePanel(): void {
    this.panelOpen.update(v => !v);
    this.tenantDropdownOpen.set(false);
  }

  closePanel(): void {
    this.panelOpen.set(false);
  }

  toggleTenantDropdown(): void {
    this.tenantDropdownOpen.update(v => !v);
  }

  selectTenant(t: Tenant): void {
    this.tenantService.setCurrentTenant(t);
    this.tenantDropdownOpen.set(false);
  }

  getLinkClass(to: string): string {
    const isActive = this.router.url === to || (to === '/' && this.router.url === '/');
    return isActive
      ? 'bg-primary/15 text-primary font-medium border-l-2 border-primary'
      : 'text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground';
  }

  getIconSvg(icon: string): string {
    const icons: Record<string, string> = {
      'layout-dashboard': '<svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="7" height="9" x="3" y="3" rx="1"/><rect width="7" height="5" x="14" y="3" rx="1"/><rect width="7" height="9" x="14" y="12" rx="1"/><rect width="7" height="5" x="3" y="16" rx="1"/></svg>',
      'database': '<svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5V19A9 3 0 0 0 21 19V5"/><path d="M3 12A9 3 0 0 0 21 12"/></svg>',
      'upload': '<svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" x2="12" y1="3" y2="15"/></svg>',
      'activity': '<svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 12h-2.48a2 2 0 0 0-1.93 1.46l-2.35 8.36a.25.25 0 0 1-.48 0L9.24 2.18a.25.25 0 0 0-.48 0l-2.35 8.36A2 2 0 0 1 4.49 12H2"/></svg>',
      'folder-open': '<svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m6 14 1.5-2.9A2 2 0 0 1 9.24 10H20a2 2 0 0 1 1.94 2.5l-1.54 6a2 2 0 0 1-1.95 1.5H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h3.9a2 2 0 0 1 1.69.9l.81 1.2a2 2 0 0 0 1.67.9H18a2 2 0 0 1 2 2v2"/></svg>',
      'bar-chart-3': '<svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18"/><path d="M18 17V9"/><path d="M13 17V5"/><path d="M8 17v-3"/></svg>',
      'settings': '<svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z"/><circle cx="12" cy="12" r="3"/></svg>',
      'help-circle': '<svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/><path d="M12 17h.01"/></svg>',
    };
    return icons[icon] || '';
  }
}
