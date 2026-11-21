import { LngLat, LngLatBounds, type Map as MapLibreMap } from "maplibre-gl";
import { sphericalCentroid, type Vec2 } from "./mercator";

export type CameraTarget = {
  center: [number, number];
  zoom: number;
  bearing?: number;
  pitch?: number;
};

export type FlightStats = {
  /** flyTo calls (one per selection that moves the camera). */
  started: number;
  /** moveend events observed after a flight. */
  finished: number;
  /** A new flight replaced one that was still running. */
  interrupted: number;
  /** moveend events of any kind (user gestures included). */
  moveends: number;
};

export const FLIGHT_DURATION_MS = 600;

function shortestLng(from: number, to: number): number {
  return ((to - from + 540) % 360) - 180;
}

/**
 * Thin wrapper over MapLibre's own camera animation.
 *
 * MapLibre already interrupts an active flyTo when a new one starts and starts
 * from the current camera. A user gesture cancels it natively, so a drag in
 * the middle of a flight wins. We only keep one target and never queue.
 */
export class CameraController {
  readonly stats: FlightStats = {
    started: 0,
    finished: 0,
    interrupted: 0,
    moveends: 0,
  };

  private readonly map: MapLibreMap;
  private flying = false;

  constructor(map: MapLibreMap) {
    this.map = map;
    this.map.on("moveend", () => {
      this.stats.moveends += 1;
      if (this.flying) {
        this.flying = false;
        this.stats.finished += 1;
      }
    });
  }

  /** True while MapLibre reports camera motion (flight, inertia or drag). */
  get active(): boolean {
    return this.map.isMoving();
  }

  /** True while a flyTo of ours has not seen its moveend yet. */
  get pending(): boolean {
    return this.flying;
  }

  /** Start or replace a flight. MapLibre starts from the current camera. */
  flyTo(target: CameraTarget, durationMs: number = FLIGHT_DURATION_MS): void {
    if (this.flying) {
      this.stats.interrupted += 1;
      this.map.stop();
      this.flying = false;
    }
    this.stats.started += 1;
    this.flying = true;
    this.map.flyTo({
      center: target.center,
      zoom: target.zoom,
      bearing: target.bearing ?? 0,
      pitch: target.pitch ?? 0,
      duration: Math.max(1, durationMs),
      essential: true,
    });
  }

  /** Immediately jumps to a target (tests and still screenshots). */
  jumpTo(target: CameraTarget): void {
    this.map.stop();
    this.flying = false;
    this.map.jumpTo({
      center: target.center,
      zoom: target.zoom,
      bearing: target.bearing ?? 0,
      pitch: target.pitch ?? 0,
    });
  }

  cancel(): void {
    if (this.flying) {
      this.flying = false;
      this.map.stop();
    }
  }

  /**
   * Fit a thread of story positions. Spherical centroid plus maximum angular
   * distance keeps antimeridian threads sane; a thread wider than a hemisphere
   * pulls back to a full-globe view rather than cropping the far side.
   */
  fitThread(points: Vec2[], weightedIndex = 0): CameraTarget {
    if (points.length === 0) return { center: [0, 20], zoom: 1.4, bearing: 0, pitch: 0 };
    const centerLngLat = sphericalCentroid(points, weightedIndex);
    if (points.length === 1) {
      return { center: centerLngLat, zoom: 3.2, bearing: 0, pitch: 0 };
    }
    let maxDistance = 0;
    for (const point of points) {
      const dx =
        shortestLng(centerLngLat[0], point[0]) * Math.cos((centerLngLat[1] * Math.PI) / 180);
      const dy = point[1] - centerLngLat[1];
      maxDistance = Math.max(maxDistance, Math.hypot(dx, dy));
    }
    const span = Math.max(1.5, maxDistance * 2);
    const minDimension = Math.min(
      this.map.getCanvas().clientWidth || 1280,
      this.map.getCanvas().clientHeight || 800,
    );
    const zoom = Math.log2((minDimension * 0.62 * 360) / (span * 256));
    const cappedZoom = maxDistance > 50 ? Math.min(zoom, 1.5) : zoom;
    return {
      center: centerLngLat,
      zoom: Math.max(1.05, Math.min(6.6, cappedZoom)),
      bearing: 0,
      pitch: 0,
    };
  }

  /** MapLibre's own bounds fit, kept for simple threads. */
  fitBounds(points: Vec2[]): CameraTarget {
    if (points.length < 2) return this.fitThread(points);
    const bounds = points.reduce(
      (acc, point) => acc.extend(point),
      new LngLatBounds(points[0], points[0]),
    );
    const camera = this.map.cameraForBounds(bounds, { padding: 120, maxZoom: 6.5 });
    if (!camera || !camera.center) return this.fitThread(points);
    const center = LngLat.convert(camera.center);
    return {
      center: [center.lng, center.lat],
      zoom: camera.zoom ?? 3,
      bearing: 0,
      pitch: 0,
    };
  }
}
