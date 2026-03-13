import { TestBed } from '@angular/core/testing';

import { Jobs } from './jobs';

describe('Jobs', () => {
  let service: Jobs;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(Jobs);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
