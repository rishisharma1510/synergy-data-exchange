import { Component } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { AppNavbarComponent } from '../app-navbar/app-navbar.component';
import { ToasterComponent } from '../toaster/toaster.component';

@Component({
  selector: 'app-layout',
  standalone: true,
  imports: [RouterOutlet, AppNavbarComponent, ToasterComponent],
  template: `
    <div class="flex flex-col min-h-screen">
      <app-navbar />
      <main class="flex-1 overflow-auto">
        <div class="max-w-5xl mx-auto p-8">
          <router-outlet />
        </div>
      </main>
      <app-toaster />
    </div>
  `,
})
export class AppLayoutComponent {}
