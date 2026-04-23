"""
AQI Calculator — direct port of the Java AqiCalculator.

Computes the Indian National AQI from individual pollutant sub-indices.
The maximum sub-index across PM2.5, PM10, NO2, SO2, CO, and O3 becomes
the overall AQI value.
"""


def _interpolate(c: float, bp_lo: float, bp_hi: float,
                 i_lo: int, i_hi: int) -> int:
    """Linear interpolation between breakpoints (mirrors Java `calculate`)."""
    return round(((i_hi - i_lo) / (bp_hi - bp_lo)) * (c - bp_lo) + i_lo)


# ── Sub-index functions ────────────────────────────────────────────

def _sub_pm25(c):
    if c is None:
        return 0
    if c <= 30:  return _interpolate(c, 0,   30,  0,   50)
    if c <= 60:  return _interpolate(c, 31,  60,  51,  100)
    if c <= 90:  return _interpolate(c, 61,  90,  101, 200)
    if c <= 120: return _interpolate(c, 91,  120, 201, 300)
    if c <= 250: return _interpolate(c, 121, 250, 301, 400)
    return            _interpolate(c, 251, 500, 401, 500)


def _sub_pm10(c):
    if c is None:
        return 0
    if c <= 50:  return _interpolate(c, 0,   50,  0,   50)
    if c <= 100: return _interpolate(c, 51,  100, 51,  100)
    if c <= 250: return _interpolate(c, 101, 250, 101, 200)
    if c <= 350: return _interpolate(c, 251, 350, 201, 300)
    if c <= 430: return _interpolate(c, 351, 430, 301, 400)
    return            _interpolate(c, 431, 600, 401, 500)


def _sub_no2(c):
    if c is None:
        return 0
    if c <= 40:  return _interpolate(c, 0,   40,  0,   50)
    if c <= 80:  return _interpolate(c, 41,  80,  51,  100)
    if c <= 180: return _interpolate(c, 81,  180, 101, 200)
    if c <= 280: return _interpolate(c, 181, 280, 201, 300)
    if c <= 400: return _interpolate(c, 281, 400, 301, 400)
    return            _interpolate(c, 401, 1000, 401, 500)


def _sub_so2(c):
    if c is None:
        return 0
    if c <= 40:   return _interpolate(c, 0,    40,   0,   50)
    if c <= 80:   return _interpolate(c, 41,   80,   51,  100)
    if c <= 380:  return _interpolate(c, 81,   380,  101, 200)
    if c <= 800:  return _interpolate(c, 381,  800,  201, 300)
    if c <= 1600: return _interpolate(c, 801,  1600, 301, 400)
    return             _interpolate(c, 1601, 2000, 401, 500)


def _sub_co(c):
    if c is None:
        return 0
    if c <= 1:  return _interpolate(c, 0,    1,  0,   50)
    if c <= 2:  return _interpolate(c, 1.1,  2,  51,  100)
    if c <= 10: return _interpolate(c, 2.1,  10, 101, 200)
    if c <= 17: return _interpolate(c, 10.1, 17, 201, 300)
    if c <= 34: return _interpolate(c, 17.1, 34, 301, 400)
    return           _interpolate(c, 34.1, 50, 401, 500)


def _sub_o3(c):
    if c is None:
        return 0
    if c <= 50:  return _interpolate(c, 0,   50,  0,   50)
    if c <= 100: return _interpolate(c, 51,  100, 51,  100)
    if c <= 168: return _interpolate(c, 101, 168, 101, 200)
    if c <= 208: return _interpolate(c, 169, 208, 201, 300)
    if c <= 748: return _interpolate(c, 209, 748, 301, 400)
    return            _interpolate(c, 749, 1000, 401, 500)


# ── Public API ─────────────────────────────────────────────────────

def compute_aqi(record: dict) -> int | None:
    """
    Compute the AQI for a reading dict (snake_case keys).
    Returns None when every sub-index is zero (all pollutants missing).
    """
    max_aqi = max(
        _sub_pm25(record.get("pm25")),
        _sub_pm10(record.get("pm10")),
        _sub_no2(record.get("no2")),
        _sub_so2(record.get("so2")),
        _sub_co(record.get("co")),
        _sub_o3(record.get("ozone")),
    )
    return max_aqi if max_aqi > 0 else None
