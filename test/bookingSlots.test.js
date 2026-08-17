import test from 'node:test';
import assert from 'node:assert/strict';
import {
  isDurationAlignedStart,
  lockStartsForRange,
  resolveAdvisorTimezone
} from '../controllers/session.controller.js';

const daytimeWindow = {
  from: '09:00',
  to: '18:00',
  fromMinutes: 9 * 60,
  toMinutes: 18 * 60
};

test('aligns available starts to the selected duration', () => {
  assert.equal(
    isDurationAlignedStart(new Date('2026-08-17T09:00:00.000Z'), 'UTC', daytimeWindow, 10),
    true
  );
  assert.equal(
    isDurationAlignedStart(new Date('2026-08-17T09:05:00.000Z'), 'UTC', daytimeWindow, 10),
    false
  );
  assert.equal(
    isDurationAlignedStart(new Date('2026-08-17T09:10:00.000Z'), 'UTC', daytimeWindow, 10),
    true
  );
});

test('uses Bangladesh local time for legacy Bangladesh advisors left at UTC', () => {
  assert.equal(
    resolveAdvisorTimezone({ country: 'BD', timezone: 'UTC' }),
    'Asia/Dhaka'
  );
  assert.equal(
    resolveAdvisorTimezone({ country: 'US', timezone: 'America/Chicago' }),
    'America/Chicago'
  );
});

test('keeps adjacent ten-minute booking locks separate', () => {
  const first = lockStartsForRange(new Date('2026-08-17T09:00:00.000Z'), 10)
    .map((value) => value.toISOString());
  const second = lockStartsForRange(new Date('2026-08-17T09:10:00.000Z'), 10)
    .map((value) => value.toISOString());

  assert.deepEqual(first, [
    '2026-08-17T09:00:00.000Z',
    '2026-08-17T09:05:00.000Z'
  ]);
  assert.deepEqual(second, [
    '2026-08-17T09:10:00.000Z',
    '2026-08-17T09:15:00.000Z'
  ]);
  assert.equal(first.some((value) => second.includes(value)), false);
});
