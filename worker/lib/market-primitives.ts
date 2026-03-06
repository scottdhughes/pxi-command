import type { CalibrationQuality } from '../types';

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const CALIBRATION_ROBUST_MIN_SAMPLE = 50;
const CALIBRATION_LIMITED_MIN_SAMPLE = 20;

export function clamp(min: number, max: number, value: number): number {
  return Math.max(min, Math.min(max, value));
}

export function asIsoDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}

export function asIsoDateTime(date: Date): string {
  return date.toISOString();
}

export function toNumber(value: unknown, fallback = 0): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export function parseIsoDate(value: string): string | null {
  if (!ISO_DATE_RE.test(value)) return null;
  const date = new Date(`${value}T00:00:00.000Z`);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString().slice(0, 10);
}

export function calibrationQualityForSampleSize(sampleSize: number): CalibrationQuality {
  if (sampleSize >= CALIBRATION_ROBUST_MIN_SAMPLE) return 'ROBUST';
  if (sampleSize >= CALIBRATION_LIMITED_MIN_SAMPLE) return 'LIMITED';
  return 'INSUFFICIENT';
}

export function stableHash(value: string): string {
  let hash = 0x811c9dc5;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = (hash * 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, '0');
}
