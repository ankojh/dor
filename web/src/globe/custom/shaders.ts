/**
 * GLSL for the custom WebGL layer.
 *
 * Projection comes from MapLibre v5's `shaderData.vertexShaderPrelude` and
 * `defaultProjectionData`: `projectTile` / `projectTileWithElevation` handle
 * globe, mercator and the transition between them on the same sphere MapLibre
 * draws, so our pins cannot drift from the basemap.
 *
 * At street zoom the globe has already switched to mercator and the prelude
 * input is absolute mercator 0..1, which float32 cannot resolve to a pixel at
 * z 17+. There we use the anchor path instead: shader positions are split into
 * a coarse multiple of 1/8192 plus a residual, the camera anchor is split the
 * same way, and the difference is multiplied by a double-precision anchor
 * matrix. Nothing large is subtracted in float32, so pins stay sub-pixel.
 */

import {
  ARC_GLOW_WIDTH_SCALE,
  ARC_MIN_STRENGTH,
  ARC_OPACITY_MAX,
  ARC_OPACITY_MIN,
  ARC_PULSE_1,
  ARC_PULSE_2,
  ARC_SECOND_ALPHA_SCALE,
  ARC_SECOND_WIDTH_SCALE,
  ARC_WIDTH_MAX,
  ARC_WIDTH_MIN,
  DEFAULT_STRENGTH_FLOOR,
  DOT_GPU_BASE,
  DOT_GPU_SCALE,
  FOCUS_AMBIENT_ALPHA,
  FOCUS_FADE_SECONDS,
  FOCUS_FOCUSED_ALPHA,
  FOCUS_LINKED_ALPHA,
  FOCUS_OFF_THREAD_ALPHA,
  FOCUS_SECOND_ALPHA,
} from "../encoding";
import {
  RIPPLE_FLASH_ALPHA,
  RIPPLE_FLASH_SECONDS,
  RIPPLE_LIFETIME_SECONDS,
  RIPPLE_OFF_THREAD_DIM,
} from "../ripples";

/**
 * One source of truth for encoding literals used in GLSL. Changing the plan's
 * thresholds in encoding.ts changes the shaders too.
 */
export const GLSL_CONSTANTS = `#define DOR_FOCUS_FADE ${FOCUS_FADE_SECONDS.toFixed(3)}
#define DOR_MIN_STRENGTH ${ARC_MIN_STRENGTH.toFixed(3)}
#define DOR_FLOOR ${DEFAULT_STRENGTH_FLOOR.toFixed(3)}
#define DOR_FOCUSED_ALPHA ${FOCUS_FOCUSED_ALPHA.toFixed(3)}
#define DOR_LINKED_ALPHA ${FOCUS_LINKED_ALPHA.toFixed(3)}
#define DOR_AMBIENT_ALPHA ${FOCUS_AMBIENT_ALPHA.toFixed(3)}
#define DOR_SECOND_ALPHA ${FOCUS_SECOND_ALPHA.toFixed(3)}
#define DOR_OFF_THREAD_ALPHA ${FOCUS_OFF_THREAD_ALPHA.toFixed(3)}
#define DOR_ARC_WIDTH_MIN ${ARC_WIDTH_MIN.toFixed(3)}
#define DOR_ARC_WIDTH_SPAN ${(ARC_WIDTH_MAX - ARC_WIDTH_MIN).toFixed(3)}
#define DOR_ARC_OPACITY_MIN ${ARC_OPACITY_MIN.toFixed(3)}
#define DOR_ARC_OPACITY_SPAN ${(ARC_OPACITY_MAX - ARC_OPACITY_MIN).toFixed(3)}
#define DOR_ARC_SECOND_WIDTH ${ARC_SECOND_WIDTH_SCALE.toFixed(3)}
#define DOR_ARC_SECOND_ALPHA ${ARC_SECOND_ALPHA_SCALE.toFixed(3)}
#define DOR_ARC_GLOW_WIDTH ${ARC_GLOW_WIDTH_SCALE.toFixed(3)}
#define DOR_DOT_BASE ${DOT_GPU_BASE.toFixed(3)}
#define DOR_DOT_SCALE ${DOT_GPU_SCALE.toFixed(3)}
#define DOR_PULSE_1 ${ARC_PULSE_1.toFixed(3)}
#define DOR_PULSE_2 ${ARC_PULSE_2.toFixed(3)}
#define DOR_RIPPLE_LIFE ${RIPPLE_LIFETIME_SECONDS.toFixed(3)}
#define DOR_RIPPLE_FLASH ${RIPPLE_FLASH_SECONDS.toFixed(3)}
#define DOR_RIPPLE_DIM ${RIPPLE_OFF_THREAD_DIM.toFixed(3)}
#define DOR_RIPPLE_FLASH_ALPHA ${RIPPLE_FLASH_ALPHA.toFixed(3)}
`;

export const COMMON_HEADER = /* glsl */ `#version 300 es
precision highp float;
precision highp sampler2D;
${GLSL_CONSTANTS}

uniform float uGlobe;
uniform float uZoom;
uniform mat4 uAnchorMatrix;
uniform vec2 uAnchorHi;
uniform vec2 uAnchorLo;
uniform vec2 uViewport;
uniform float uTime;
uniform float uFocusAt;
uniform float uStoryTexWidth;
uniform float uLinkTexWidth;
uniform float uHasFocus;
uniform float uFloor;
uniform float uDropGlow;
uniform float uDropPulses;
uniform float uDropThematic;
uniform float uHeatOn;

vec4 dorProject(vec2 hi, vec2 lo, float elevation) {
  if (uGlobe > 0.5) {
    return projectTileWithElevation(hi + lo, elevation);
  }
  // Mercator custom-layer z is conformal mercator units, not metres; the
  // surface lift only matters on the globe, so flatten it here.
  vec2 delta = (hi - uAnchorHi) + (lo - uAnchorLo);
  return uAnchorMatrix * vec4(delta, 0.0, 1.0);
}

ivec2 dorTexel(float index, float width) {
  int i = int(index);
  int w = max(1, int(width));
  return ivec2(i % w, i / w);
}

bool dorClipped(vec4 clip) {
  if (clip.w == 0.0) return true;
  if (uGlobe > 0.5) {
    float ndcZ = clip.z / clip.w;
    return ndcZ < -1.0 || ndcZ > 1.0;
  }
  return false;
}

float dorFocusOpacity(float state) {
  if (state > 1.5 && state < 2.5) return DOR_FOCUSED_ALPHA;
  if (state > 0.5 && state < 1.5) return DOR_LINKED_ALPHA;
  if (state > 2.5) return DOR_SECOND_ALPHA;
  // Ambient stories do not get a per-instance focus texel, so the global
  // off-thread dim must fade too (200 ms, plan §4). A focus toggle is the only
  // transition; switching story while focused leaves the level at its target.
  float target = uHasFocus;
  float start = 1.0 - target;
  float level = mix(start, target, clamp((uTime - uFocusAt) / DOR_FOCUS_FADE, 0.0, 1.0));
  return mix(1.0, DOR_AMBIENT_ALPHA, level);
}

/** Shared by the visible and ID passes so click targets match what is drawn. */
float dorArcZoomVisibility(float arcLength) {
  float arcKm = arcLength * 0.001;
  float maxArcKm = uZoom > 13.0 ? 2.0 : uZoom > 10.0 ? 25.0 : uZoom > 8.0 ? 250.0 : 14000.0;
  return 1.0 - smoothstep(maxArcKm * 0.8, maxArcKm, arcKm);
}

float dorArcStreetVisibility(float strength) {
  return uZoom > 13.0 ? smoothstep(0.52, 0.68, strength) : 1.0;
}
`;

export const DOT_VERT = /* glsl */ `${COMMON_HEADER}
layout(location = 0) in vec2 aMercHi;
layout(location = 1) in vec2 aMercLo;
layout(location = 2) in float aIndex;
layout(location = 3) in vec3 aColor;
layout(location = 4) in vec2 aCorner;

uniform sampler2D uStoryTex;
uniform sampler2D uFocusTex;

out vec2 vLocal;
out float vRadius;
out float vAlpha;
out float vHeat;
out float vPhase;
out float vFocused;
out float vLive;
out vec3 vColor;

void main() {
  vec4 story = texelFetch(uStoryTex, dorTexel(aIndex, uStoryTexWidth), 0);
  vec4 focus = texelFetch(uFocusTex, dorTexel(aIndex, uStoryTexWidth), 0);
  float state = focus.x;
  float prev = focus.y;
  float fade = clamp((uTime - focus.z) / DOR_FOCUS_FADE, 0.0, 1.0);
  float alpha = mix(dorFocusOpacity(prev), dorFocusOpacity(state), fade);
  alpha *= focus.w > 0.5 ? 0.12 : 1.0;

  float radius = (DOR_DOT_BASE + DOR_DOT_SCALE * story.x) *
    mix(0.64, 1.28, smoothstep(1.2, 8.0, uZoom));
  float focused = step(1.5, state) * step(state, 2.5);
  float quad = max(radius * 1.7 + 4.0, radius + focused * 18.0);
  // World view: keep the hot stories, fade the long tail so 5k dots do not
  // turn the globe into a white ball.
  float lowZoom = 1.0 - smoothstep(2.5, 6.0, uZoom);
  float heat = story.y * uHeatOn;
  alpha *= mix(1.0, 0.5 + 0.5 * smoothstep(0.2, 0.6, heat), lowZoom);
  // Stories cool slowly; a brand-new story fades in over ~1.2 s.
  float storyAge = max(0.0, uTime - story.z);
  alpha *= mix(0.55, 1.0, exp(-storyAge / 3600.0));
  alpha *= mix(0.25, 1.0, smoothstep(0.0, 1.2, storyAge));

  vec4 clip = dorProject(aMercHi, aMercLo, 4.0);
  // story.x is the size factor; a freed runtime slot is zero.
  if (story.x <= 0.001 || dorClipped(clip) || alpha < 0.004) {
    gl_Position = vec4(2.0, 2.0, 2.0, 0.0);
    return;
  }
  vec2 ndc = clip.xy / clip.w;
  gl_Position = vec4(ndc + aCorner * quad * 2.0 / uViewport, clip.z / clip.w, 1.0);

  vLocal = aCorner * quad;
  vRadius = radius;
  vAlpha = alpha;
  vHeat = heat;
  vPhase = fract(sin(aIndex * 12.9898) * 43758.5453);
  vFocused = focused;
  vLive = exp(-max(0.0, uTime - story.z) / 6.0);
  vColor = aColor;
}
`;

export const DOT_FRAG = /* glsl */ `#version 300 es
precision highp float;
${GLSL_CONSTANTS}

uniform float uTime;
uniform float uDropGlow;

in vec2 vLocal;
in float vRadius;
in float vAlpha;
in float vHeat;
in float vPhase;
in float vFocused;
in float vLive;
in vec3 vColor;

out vec4 outColor;

void main() {
  float d = length(vLocal);
  float r = vRadius;
  float core = 1.0 - smoothstep(r - 1.0, r + 0.8, d);
  float white = 1.0 - smoothstep(0.0, r * 0.3, d);
  float glow = exp(-max(0.0, d - r) / (r * 0.45 + 1.4));
  float phase = fract(uTime * 0.45 + vPhase);
  float ringRadius = r + phase * (4.0 + 6.0 * vHeat);
  float ring = (1.0 - smoothstep(0.6, 1.6, abs(d - ringRadius))) * (1.0 - phase);

  float bracket = 0.0;
  if (vFocused > 0.5) {
    vec2 q = abs(vLocal);
    float b = r + 13.0;
    float horizontal =
      step(b - 9.0, q.x) * step(q.x, b) * step(b - 1.2, q.y) * step(q.y, b + 1.2);
    float vertical =
      step(b - 1.2, q.x) * step(q.x, b + 1.2) * step(b - 9.0, q.y) * step(q.y, b);
    bracket = max(horizontal, vertical);
  }

  float alpha =
    core * vAlpha * 0.92 +
    white * vAlpha * 0.82 +
    glow * 0.1 * vAlpha * (1.0 - uDropGlow) +
    ring * 0.22 * vLive * vAlpha +
    bracket * 0.9;
  if (alpha < 0.004) discard;

  vec3 color = mix(vColor, vec3(1.0), white * 0.9 + bracket * 0.9);
  // Additive-ish glow: keep the core opaque, let halo contribute light.
  outColor = vec4(color, min(1.0, alpha));
}
`;

export const ARC_VERT = /* glsl */ `${COMMON_HEADER}
layout(location = 0) in vec2 aStartHi;
layout(location = 1) in vec2 aStartLo;
layout(location = 2) in vec2 aEndHi;
layout(location = 3) in vec2 aEndLo;
layout(location = 4) in float aStartElev;
layout(location = 5) in float aEndElev;
layout(location = 6) in float aT0;
layout(location = 7) in float aT1;
layout(location = 8) in float aLink;
layout(location = 9) in float aType;
layout(location = 10) in vec2 aCorner;
layout(location = 11) in float aArcLength;

uniform sampler2D uArcFocusTex;
uniform sampler2D uArcDirTex;
uniform vec3 uLinkColors[3];

out float vSidePx;
out float vCorePx;
out float vT;
out float vAlpha;
out float vStrength;
out float vDashed;
out float vPulse;
out float vDirection;
out float vSeed;
out vec3 vColor;

void main() {
  vec4 focus = texelFetch(uArcFocusTex, dorTexel(aLink, uLinkTexWidth), 0);
  float direction = texelFetch(uArcDirTex, dorTexel(aLink, uLinkTexWidth), 0).x;
  float state = focus.x;
  float prev = focus.y;
  float at = focus.z;
  float strength = abs(focus.w);
  float fade = clamp((uTime - at) / DOR_FOCUS_FADE, 0.0, 1.0);
  float alpha = mix(dorFocusOpacity(prev), dorFocusOpacity(state), fade) * (focus.w < 0.0 ? 0.12 : 1.0);

  float second = step(2.5, state);
  float ambient = 1.0 - step(0.5, state);
  float width = (DOR_ARC_WIDTH_MIN + DOR_ARC_WIDTH_SPAN * strength) *
    mix(1.0, DOR_ARC_SECOND_WIDTH, second);
  alpha *= (DOR_ARC_OPACITY_MIN + DOR_ARC_OPACITY_SPAN * strength) *
    mix(1.0, DOR_ARC_SECOND_ALPHA, second);
  // Board A2: off-thread arcs stay as a very faint context web.
  if (strength < DOR_MIN_STRENGTH) alpha = 0.0;
  if (uHasFocus < 0.5 && strength < uFloor) alpha = 0.0;
  if (ambient > 0.5 && uHasFocus > 0.5) alpha *= DOR_OFF_THREAD_ALPHA;

  vec4 c0 = dorProject(aStartHi, aStartLo, aStartElev);
  vec4 c1 = dorProject(aEndHi, aEndLo, aEndElev);
  if (dorClipped(c0) || dorClipped(c1)) {
    gl_Position = vec4(2.0, 2.0, 2.0, 0.0);
    return;
  }

  vec2 scr0 = (c0.xy / c0.w) * 0.5 * uViewport;
  vec2 scr1 = (c1.xy / c1.w) * 0.5 * uViewport;
  vec2 dir = scr1 - scr0;
  float len = length(dir);
  // At street zoom a link between cities would be a screen-long line; keep
  // only links that are local to the viewport, tapering with zoom.
  alpha *= dorArcZoomVisibility(aArcLength);
  alpha *= dorArcStreetVisibility(strength);
  if (alpha < 0.004) {
    gl_Position = vec4(2.0, 2.0, 2.0, 0.0);
    return;
  }
  vec2 normal = len > 0.001 ? vec2(-dir.y, dir.x) / len : vec2(0.0, 1.0);
  float glowWidth = max(width * DOR_ARC_GLOW_WIDTH, width + 1.5);
  vec2 base = mix(scr0, scr1, aCorner.x);
  vec2 pos = base + normal * aCorner.y * glowWidth * 0.5;
  vec4 clip = mix(c0, c1, aCorner.x);
  gl_Position = vec4(pos / (0.5 * uViewport) * clip.w, clip.z, clip.w);

  vSidePx = aCorner.y * glowWidth * 0.5;
  vCorePx = width;
  vT = mix(aT0, aT1, aCorner.x);
  vAlpha = alpha;
  vStrength = strength;
  vDashed = step(1.5, aType);
  vDirection = direction;
  vPulse = (1.0 - vDashed) * (1.0 - second) * step(DOR_PULSE_1, strength) *
    (1.0 - (1.0 - step(0.5, state)) * uHasFocus) * (1.0 - uDropPulses);
  if (uDropThematic > 0.5 && aType > 1.5) alpha = 0.0;
  vSeed = fract(sin(aLink * 3.71) * 43758.5453);
  vColor = uLinkColors[int(aType)];
}
`;

export const ARC_FRAG = /* glsl */ `#version 300 es
precision highp float;
${GLSL_CONSTANTS}

uniform float uTime;

in float vSidePx;
in float vCorePx;
in float vT;
in float vAlpha;
in float vStrength;
in float vDashed;
in float vPulse;
in float vDirection;
in float vSeed;
in vec3 vColor;


out vec4 outColor;

void main() {
  float d = abs(vSidePx);
  float halfCore = max(0.35, vCorePx * 0.5);
  float core = 1.0 - smoothstep(halfCore - 0.6, halfCore + 0.6, d);
  float glow = exp(-max(0.0, d - halfCore) / (halfCore + 1.6)) * (0.2 + 0.3 * vStrength);
  float alpha = (core + glow * 0.45) * vAlpha;

  float t = vT;
  if (vDashed > 0.5 && fract(t * 55.0 + vSeed) > 0.62) alpha = 0.0;
  if (alpha < 0.004) discard;

  float pulse = 0.0;
  // Direction says which story acted on the other. UNCLEAR (2) never pulses.
  if (vPulse > 0.5 && vDirection < 1.5) {
    float along = vDirection > 0.5 ? 1.0 - vT : vT;
    float speed = 0.15 + 0.35 * vStrength;
    float count = vStrength >= DOR_PULSE_2 ? 2.0 : 1.0;
    for (int i = 0; i < 2; i++) {
      if (float(i) >= count) break;
      float p = fract(uTime * speed + vSeed + float(i) / count);
      float delta = abs(along - p);
      delta = min(delta, 1.0 - delta);
      pulse = max(pulse, (1.0 - smoothstep(0.004, 0.014, delta)) * vAlpha);
    }
  }

  vec3 color = mix(vColor, vec3(1.0), pulse);
  alpha = max(alpha, pulse);
  if (alpha < 0.004) discard;
  outColor = vec4(color, min(1.0, alpha));
}
`;

export const RIPPLE_VERT = /* glsl */ `${COMMON_HEADER}
layout(location = 0) in vec2 aMercHi;
layout(location = 1) in vec2 aMercLo;
layout(location = 2) in float aCategory;
layout(location = 3) in float aSpawnAt;
layout(location = 4) in float aIntensity;
layout(location = 5) in vec2 aCorner;
layout(location = 6) in float aStoryIndex;

uniform vec3 uCategoryColors[11];
uniform sampler2D uFocusTex;

out vec2 vLocal;
out float vRadius;
out float vAge;
out float vIntensity;
out float vDim;
out vec3 vColor;

void main() {
  float age = uTime - aSpawnAt;
  if (age < 0.0 || age > DOR_RIPPLE_LIFE) {
    gl_Position = vec4(2.0, 2.0, 2.0, 0.0);
    return;
  }
  float k = clamp(age / DOR_RIPPLE_LIFE, 0.0, 1.0);
  float ease = 1.0 - pow(1.0 - k, 3.0);
  float radius = 4.0 + ease * (22.0 + 10.0 * aIntensity);
  float quad = radius + 5.0;

  vec4 clip = dorProject(aMercHi, aMercLo, 2.0);
  if (dorClipped(clip)) {
    gl_Position = vec4(2.0, 2.0, 2.0, 0.0);
    return;
  }
  vec2 ndc = clip.xy / clip.w;
  gl_Position = vec4(ndc + aCorner * quad * 2.0 / uViewport, clip.z / clip.w, 1.0);

  vLocal = aCorner * quad;
  vRadius = radius;
  vAge = age;
  vIntensity = aIntensity;
  vDim = 1.0;
  if (uHasFocus > 0.5) {
    int story = max(0, int(aStoryIndex));
    vec4 focus = texelFetch(uFocusTex, dorTexel(float(story), uStoryTexWidth), 0);
    if (focus.x < 0.5) vDim = DOR_RIPPLE_DIM;
  }
  vColor = uCategoryColors[int(aCategory)];
}
`;

export const RIPPLE_FRAG = /* glsl */ `#version 300 es
precision highp float;
${GLSL_CONSTANTS}

in vec2 vLocal;
in float vRadius;
in float vAge;
in float vIntensity;
in float vDim;
in vec3 vColor;

out vec4 outColor;

void main() {
  float d = length(vLocal);
  float ring = 1.0 - smoothstep(0.7, 1.8, abs(d - vRadius));
  float k = clamp(vAge / DOR_RIPPLE_LIFE, 0.0, 1.0);
  float alpha = ring * (1.0 - k) * (0.55 + 0.45 * vIntensity) * vDim;
  vec3 color = vColor;
  if (vAge < DOR_RIPPLE_FLASH) {
    float flash = (1.0 - smoothstep(0.0, DOR_RIPPLE_FLASH, vAge)) * (1.0 - smoothstep(0.0, 7.0, d));
    // The white spawn flash must respect the same off-thread muting as the
    // ring itself; otherwise a focused view flashes for every article.
    alpha = max(alpha, flash * DOR_RIPPLE_FLASH_ALPHA * vDim);
    color = mix(color, vec3(1.0), flash * vDim);
  }
  if (alpha < 0.004) discard;
  outColor = vec4(color, min(1.0, alpha));
}
`;

export const TETHER_VERT = /* glsl */ `${COMMON_HEADER}
layout(location = 0) in vec2 aStartHi;
layout(location = 1) in vec2 aStartLo;
layout(location = 2) in vec2 aEndHi;
layout(location = 3) in vec2 aEndLo;
layout(location = 4) in float aT0;
layout(location = 5) in float aT1;
layout(location = 6) in vec2 aCorner;

out float vSidePx;
out float vT;

void main() {
  vec4 c0 = dorProject(aStartHi, aStartLo, 3.0);
  vec4 c1 = dorProject(aEndHi, aEndLo, 3.0);
  if (dorClipped(c0) || dorClipped(c1)) {
    gl_Position = vec4(2.0, 2.0, 2.0, 0.0);
    return;
  }
  vec2 scr0 = (c0.xy / c0.w) * 0.5 * uViewport;
  vec2 scr1 = (c1.xy / c1.w) * 0.5 * uViewport;
  vec2 dir = scr1 - scr0;
  float len = length(dir);
  vec2 normal = len > 0.001 ? vec2(-dir.y, dir.x) / len : vec2(0.0, 1.0);
  vec2 base = mix(scr0, scr1, aCorner.x);
  vec2 pos = base + normal * aCorner.y * 0.9;
  float z = mix(c0.z / c0.w, c1.z / c1.w, aCorner.x);
  gl_Position = vec4(pos / (0.5 * uViewport), z, 1.0);
  vSidePx = aCorner.y * 0.9;
  vT = mix(aT0, aT1, aCorner.x);
}
`;

export const TETHER_FRAG = /* glsl */ `#version 300 es
precision highp float;

in float vSidePx;
in float vT;

out vec4 outColor;

void main() {
  if (fract(vT * 52.0) > 0.42) discard;
  float edge = 1.0 - smoothstep(0.5, 1.0, abs(vSidePx));
  if (edge < 0.05) discard;
  outColor = vec4(0.93, 0.97, 1.0, 0.72 * edge);
}
`;

export const RING_VERT = /* glsl */ `${COMMON_HEADER}
layout(location = 0) in vec2 aHi;
layout(location = 1) in vec2 aLo;
layout(location = 2) in vec2 aCorner;

out vec2 vLocal;

void main() {
  vec4 clip = dorProject(aHi, aLo, 3.0);
  if (dorClipped(clip)) {
    gl_Position = vec4(2.0, 2.0, 2.0, 0.0);
    return;
  }
  vec2 ndc = clip.xy / clip.w;
  gl_Position = vec4(ndc + aCorner * 9.0 * 2.0 / uViewport, clip.z / clip.w, 1.0);
  vLocal = aCorner * 9.0;
}
`;

export const RING_FRAG = /* glsl */ `#version 300 es
precision highp float;

in vec2 vLocal;

out vec4 outColor;

void main() {
  float d = length(vLocal);
  float ring = 1.0 - smoothstep(0.6, 1.5, abs(d - 6.4));
  float dot = 1.0 - smoothstep(0.0, 1.8, d);
  float alpha = max(ring, dot * 0.9);
  if (alpha < 0.004) discard;
  outColor = vec4(1.0, 1.0, 1.0, alpha * 0.95);
}
`;

export const FULLSCREEN_DOT_VERT = DOT_VERT;

export const PICK_VERT = /* glsl */ `${COMMON_HEADER}
layout(location = 0) in vec2 aMercHi;
layout(location = 1) in vec2 aMercLo;
layout(location = 2) in float aIndex;
layout(location = 4) in vec2 aCorner;

uniform sampler2D uStoryTex;

out vec2 vLocal;
out float vRadius;
out float vId;

void main() {
  vec4 story = texelFetch(uStoryTex, dorTexel(aIndex, uStoryTexWidth), 0);
  float radius = max(4.0, (DOR_DOT_BASE + DOR_DOT_SCALE * story.x) * 1.28);
  if (story.x <= 0.001) {
    gl_Position = vec4(2.0, 2.0, 2.0, 0.0);
    return;
  }
  vec4 clip = dorProject(aMercHi, aMercLo, 0.0);
  if (dorClipped(clip)) {
    gl_Position = vec4(2.0, 2.0, 2.0, 0.0);
    return;
  }
  vec2 ndc = clip.xy / clip.w;
  float quad = radius + 2.0;
  gl_Position = vec4(ndc + aCorner * quad * 2.0 / uViewport, clip.z / clip.w, 1.0);
  vLocal = aCorner * quad;
  vRadius = radius;
  vId = aIndex + 1.0;
}
`;

export const PICK_FRAG = /* glsl */ `#version 300 es
precision highp float;

in vec2 vLocal;
in float vRadius;
in float vId;

out vec4 outColor;

void main() {
  if (length(vLocal) > vRadius) discard;
  float id = vId;
  float r = mod(id, 256.0);
  float g = mod(floor(id / 256.0), 256.0);
  float b = floor(id / 65536.0);
  outColor = vec4(r / 255.0, g / 255.0, b / 255.0, 1.0);
}
`;

/**
 * Arc id pass. It mirrors the visible arc pass (same projection, width, focus
 * and floor rules) so focused links below the ambient floor stay clickable and
 * links hidden by the floor, quality level or street-zoom attenuation are not.
 */
export const PICK_ARC_VERT = /* glsl */ `${COMMON_HEADER}
layout(location = 0) in vec2 aStartHi;
layout(location = 1) in vec2 aStartLo;
layout(location = 2) in vec2 aEndHi;
layout(location = 3) in vec2 aEndLo;
layout(location = 4) in float aStartElev;
layout(location = 5) in float aEndElev;
layout(location = 6) in float aT0;
layout(location = 7) in float aT1;
layout(location = 8) in float aLink;
layout(location = 9) in float aType;
layout(location = 10) in vec2 aCorner;
layout(location = 11) in float aArcLength;

uniform sampler2D uArcFocusTex;

out float vSidePx;
out float vId;

void main() {
  vec4 focus = texelFetch(uArcFocusTex, dorTexel(aLink, uLinkTexWidth), 0);
  float state = focus.x;
  float strength = abs(focus.w);
  float second = step(2.5, state);
  float alpha = (DOR_ARC_OPACITY_MIN + DOR_ARC_OPACITY_SPAN * strength) *
    mix(1.0, DOR_ARC_SECOND_ALPHA, second);
  if (strength < DOR_MIN_STRENGTH) alpha = 0.0;
  if (uHasFocus < 0.5 && strength < uFloor) alpha = 0.0;
  if (uDropThematic > 0.5 && aType > 1.5) alpha = 0.0;
  alpha *= dorArcZoomVisibility(aArcLength);
  alpha *= dorArcStreetVisibility(strength);
  if (alpha < 0.004) {
    gl_Position = vec4(2.0, 2.0, 2.0, 0.0);
    return;
  }

  vec4 c0 = dorProject(aStartHi, aStartLo, aStartElev);
  vec4 c1 = dorProject(aEndHi, aEndLo, aEndElev);
  if (dorClipped(c0) || dorClipped(c1)) {
    gl_Position = vec4(2.0, 2.0, 2.0, 0.0);
    return;
  }

  vec2 scr0 = (c0.xy / c0.w) * 0.5 * uViewport;
  vec2 scr1 = (c1.xy / c1.w) * 0.5 * uViewport;
  vec2 dir = scr1 - scr0;
  float len = length(dir);
  vec2 normal = len > 0.001 ? vec2(-dir.y, dir.x) / len : vec2(0.0, 1.0);
  float width = (DOR_ARC_WIDTH_MIN + DOR_ARC_WIDTH_SPAN * strength) *
    mix(1.0, DOR_ARC_SECOND_WIDTH, second);
  // A click should not need pixel-perfect aim: widen the hit corridor by 3 px
  // on each side, like a touch target.
  float pickWidth = max(5.0, width + 6.0);
  vec2 base = mix(scr0, scr1, aCorner.x);
  vec2 pos = base + normal * aCorner.y * pickWidth * 0.5;
  vec4 clip = mix(c0, c1, aCorner.x);
  gl_Position = vec4(pos / (0.5 * uViewport) * clip.w, clip.z, clip.w);
  vSidePx = aCorner.y * pickWidth * 0.5;
  vId = aLink + 1.0;
}
`;

export const PICK_ARC_FRAG = /* glsl */ `#version 300 es
precision highp float;

in float vSidePx;
in float vId;

out vec4 outColor;

void main() {
  float id = vId;
  float r = mod(id, 256.0);
  float g = mod(floor(id / 256.0), 256.0);
  float b = floor(id / 65536.0);
  outColor = vec4(r / 255.0, g / 255.0, b / 255.0, 1.0);
}
`;
