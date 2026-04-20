"""
demand_model.py — Madrid traffic and demand models.

Contains:
· TrafficModel — Madrid peak hours + vehicle multipliers
· DemandModel — hourly distribution, Monday-Friday/weekend differences,
demand skewness by zone

MADRID TRAFFIC:

Peak hours: 8:00–10:00 AM (morning), 2:00–4:00 PM (midday), 6:00–9:00 PM (evening).
Differentiated impact by vehicle: CAR > SCOOTER >> BIKE.

The generated event_times directly reflect these delays, allowing calculations in the layer 
Analysis of how many orders arrive late, by vehicle and zone.

DEMAND MODELING (Realism):
Gaussian curves for lunch (1:30 PM) and dinner (9:00 PM) as a base,

combined with surge multipliers, promotion periods, and Poisson noise.
Sampling of zones with weights → skewness at the zone level.
"""

import math
import random
from datetime import datetime
from typing import Dict, List, Tuple

from config import Config
from models import MADRID_ZONES, ZONE_MAP, VehicleType

class TrafficModel:
    """
    ╔══════════════════════════════════════════════════════════════════════╗
    ║  Traffic and delays according to vehicle type in Madrid              ║
    ║                                                                      ║
    ║  Real peak hours in Madrid:                                          ║
    ║    · Morning:   08:00 – 10:00                                        ║
    ║    · Mid-day: 14:00 – 16:00                                          ║
    ║    · Afternoon:    18:00 – 21:00                                     ║
    ║                                                                      ║
    ║  Vehicle impact:                                                     ║
    ║    · CAR:     HIGHLY affected   → up to x2.8 multiplier in peak      ║
    ║    · SCOOTER: Some effect → up to ×1.6 multiplier in peak            ║
    ║    · BIKE:    Almost no effect    → max ×1.1 multiplier              ║
    ╚══════════════════════════════════════════════════════════════════════╝
    """
    RUSH_HOURS: List[Tuple[int, int, str]] = [
        (8,  10, "morning_rush"),
        (14, 16, "lunch_rush"),
        (18, 21, "evening_rush"),
    ]

    VEHICLE_DELAY_MULTIPLIERS: Dict[str, Dict[str, float]] = {
        VehicleType.CAR.value: {
            "rush":   2.8,   
            "normal": 1.0,
        },
        VehicleType.SCOOTER.value: {
            "rush":   1.6,   
            "normal": 1.0,
        },
        VehicleType.BIKE.value: {
            "rush":   1.1,   
            "normal": 1.0,
        },
    }

    # Probability of suffering an extra delay (>10 min)
    EXTRA_DELAY_PROBABILITY: Dict[str, float] = {
        VehicleType.CAR.value:     0.65,
        VehicleType.SCOOTER.value: 0.35,
        VehicleType.BIKE.value:    0.08,
    }

    @classmethod
    def is_rush_hour(cls, dt: datetime) -> Tuple[bool, str]:
        """Gives (True, nombre_rush)if it is peak hour in Madrid."""
        for start, end, name in cls.RUSH_HOURS:
            if start <= dt.hour < end:
                return True, name
        return False, ""

    @classmethod
    def get_transit_seconds(cls, dt: datetime, vehicle_type: str) -> float:
        """
        Calculate the transit time in seconds affected by Madrid traffic based on vehicle type and time of day.
        This method is the core of delay tracking: the generated event_times will directly reflect the impact of 
        traffic, enabling the calculation of late orders per vehicle/zone in the analytics layer.
        """
        is_rush, _ = cls.is_rush_hour(dt)
        mode = "rush" if is_rush else "normal"

        # Base transit time without traffic: 5–15 min
        base_transit_s = random.uniform(300.0, 900.0)

        vehicle_multipliers = cls.VEHICLE_DELAY_MULTIPLIERS.get(
            vehicle_type,
            cls.VEHICLE_DELAY_MULTIPLIERS[VehicleType.SCOOTER.value],
        )
        transit_s = base_transit_s * vehicle_multipliers[mode]

        # Additional probabilistic delay in rush (temporary traffic jams)
        if is_rush:
            prob_extra = cls.EXTRA_DELAY_PROBABILITY.get(vehicle_type, 0.3)
            if random.random() < prob_extra:
                extra_s = random.uniform(300.0, 1200.0)   # 5–20 min extra
                if vehicle_type == VehicleType.CAR.value:
                    extra_s *= 1.5   
                transit_s += extra_s

        return transit_s

    @classmethod
    def get_prep_seconds(cls) -> float:
        """Kitchen preparation time (independent of traffic)."""
        return random.uniform(600.0, 1800.0)   # 10–30 min


# =============================================================================
# DEMAND MODEL
# =============================================================================

class DemandModel:
    """
    ╔══════════════════════════════════════════════════════════════════════╗
    ║  Realistic Demand Model                                              ║
    ║    · Schedule with lunch and dinner peaks                            ║
    ║    · Differences between weekdays and weekends (+40% increase)       ║
    ║    · Demand skew by area of ​​Madrid                                   ║
    ╚══════════════════════════════════════════════════════════════════════╝
    """

    def __init__(self, config: Config):
        self.config        = config
        self._hourly_dist  = self._build_hourly_distribution()
        self._zone_ids     = [z.zone_id      for z in MADRID_ZONES]
        self._zone_weights = [z.demand_weight for z in MADRID_ZONES]

    # ── Hourly Distribution ──────────────────────────────────────────────

    def _build_hourly_distribution(self) -> Dict[int, float]:
        """
        Construct the normalized distribution of orders per hour using:
        • Lunch Gaussian: μ = 1:30 PM, σ = 1.2 hours
        • Dinner Gaussian: μ = 9:00 PM, σ = 2.0 hours
        • Residual nighttime activity: base = 0.08
        • Surge multipliers from Config
        • Multipliers from active promotion periods
        """
        dist: Dict[int, float] = {}
        for h in range(24):
            lunch  = math.exp(-0.5 * ((h - 13.5) / 1.2) ** 2)
            dinner = math.exp(-0.5 * ((h - 21.0) / 2.0) ** 2)
            base   = 0.08
            value  = base + 0.7 * lunch + 1.0 * dinner

            surge = self.config.demand_surge_multipliers.get(h, 1.0)

            promo_mult = 1.0
            for p in self.config.promotion_periods:
                if p.start_hour <= h < p.end_hour:
                    promo_mult = max(promo_mult, p.demand_multiplier)

            dist[h] = value * surge * promo_mult

        total = sum(dist.values())
        return {h: v / total for h, v in dist.items()}

    def get_orders_for_hour(self, dt: datetime) -> int:
        """
        Number of orders to be generated in a specific hour.
        Applies hourly distribution, weekend factor, and Poisson noise.
        """
        weight   = self._hourly_dist.get(dt.hour, 1 / 24)
        expected = self.config.num_orders * weight
        if dt.weekday() >= 5:   # saturday=5, sunday=6
            expected *= self.config.weekend_demand_multiplier
        return max(1, int(random.gauss(expected, expected * 0.15)))

    # ── Zone Sample ───────────────────────────────────────────────────

    def sample_zone(self) -> str:
        """
        Weighted sampling of a Madrid area.
        Centro and Salamanca have a much higher probability than Vallecas,
        modeling the city's actual demand skew.
        """
        return random.choices(self._zone_ids, weights=self._zone_weights, k=1)[0]

    def sample_coords_in_zone(self, zone_id: str) -> Tuple[float, float]:
        """Generates random coordinates within the boundaries of a zone."""
        zone = ZONE_MAP.get(zone_id, MADRID_ZONES[0])
        lat  = zone.lat_center + random.uniform(-zone.lat_radius, zone.lat_radius)
        lon  = zone.lon_center + random.uniform(-zone.lon_radius, zone.lon_radius)
        return round(lat, 6), round(lon, 6)
