import { Routes } from '@angular/router';
import { AppLayoutComponent } from './components/app-layout/app-layout.component';

export const routes: Routes = [
  {
    path: '',
    component: AppLayoutComponent,
    children: [
      {
        path: '',
        loadComponent: () => import('./pages/dashboard/dashboard.component').then(m => m.DashboardComponent),
      },
      {
        path: 'extraction',
        loadComponent: () => import('./pages/extraction/extraction.component').then(m => m.ExtractionComponent),
      },
      {
        path: 'ingestion',
        loadComponent: () => import('./pages/ingestion/ingestion.component').then(m => m.IngestionComponent),
      },
      {
        path: 'activity',
        loadComponent: () => import('./pages/activity/activity.component').then(m => m.ActivityComponent),
      },
    ],
  },
  {
    path: '**',
    loadComponent: () => import('./pages/not-found/not-found.component').then(m => m.NotFoundComponent),
  },
];
