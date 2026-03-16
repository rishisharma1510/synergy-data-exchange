import { Injectable, signal, computed } from '@angular/core';

export interface Tenant {
  id: string;
  name: string;
  tenantId: string;
}

const TENANTS: Tenant[] = [
  { id: 'ssautomate03', name: 'ssautoma03', tenantId: '42a5bbbe-6b21-4ff2-a9e2-ecff223be59d' },
  { id: 'ruby000001',   name: 'ruby000001', tenantId: 'f9fe6757-8a50-413f-a91e-6458d362a9ce' },
  { id: 'bridgetest',   name: 'bridgete01', tenantId: 'ad391d26-4339-4e5b-9813-217f1b16287a' },
  { id: 'ssautoma01',   name: 'ssautoma01', tenantId: 'd19f972a-5ac2-45c0-aba9-391229db6ac3' },
  { id: 'polarisl01',   name: 'polarisl01', tenantId: '5a26a629-aa92-492a-a3d3-6ecfa6e1be06' },
];

function getPersistedTenant(): Tenant {
  try {
    const stored = localStorage.getItem('selectedTenantId');
    if (stored) {
      const found = TENANTS.find(t => t.id === stored);
      if (found) return found;
    }
  } catch {}
  return TENANTS[2]; // bridgete01 default
}

@Injectable({ providedIn: 'root' })
export class TenantService {
  readonly tenants = TENANTS;
  private _currentTenant = signal<Tenant>(getPersistedTenant());

  readonly currentTenant = this._currentTenant.asReadonly();
  readonly tenantId = computed(() => this._currentTenant().tenantId);
  readonly tenantResourceId = computed(() => this._currentTenant().name);

  setCurrentTenant(tenant: Tenant): void {
    this._currentTenant.set(tenant);
    try { localStorage.setItem('selectedTenantId', tenant.id); } catch {}
  }
}
