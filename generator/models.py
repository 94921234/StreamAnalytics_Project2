"""
models.py — Enumerations, domain entities, and resource pool.

Contains:
  · Event type enums (OrderEventType, CourierEventType, VehicleType)
  · MadridZone dataclass with coordinates and demand weight
  · MADRID_ZONES / ZONE_MAP — catalog of real zones in Madrid
  · EntityPool — management of the restaurant and courier pool
"""

import random
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional

from config import Config


# =============================================================================
# ENUMS OF DOMAIN
# =============================================================================

class OrderEventType(str, Enum):
    ORDER_CREATED    = "ORDER_CREATED"
    ORDER_ACCEPTED   = "ORDER_ACCEPTED"
    PREP_STARTED     = "PREP_STARTED"
    READY_FOR_PICKUP = "READY_FOR_PICKUP"
    PICKED_UP        = "PICKED_UP"
    DELIVERED        = "DELIVERED"
    CANCELLED        = "CANCELLED"


class CourierEventType(str, Enum):
    COURIER_ONLINE          = "COURIER_ONLINE"
    COURIER_OFFLINE         = "COURIER_OFFLINE"
    COURIER_ASSIGNED        = "COURIER_ASSIGNED"
    COURIER_ARRIVED_PICKUP  = "COURIER_ARRIVED_PICKUP"
    COURIER_ARRIVED_DROPOFF = "COURIER_ARRIVED_DROPOFF"
    COURIER_LOCATION        = "COURIER_LOCATION"


class VehicleType(str, Enum):
    BIKE    = "BIKE"
    SCOOTER = "SCOOTER"
    CAR     = "CAR"


# =============================================================================
# AREAS OF MADRID
# =============================================================================

@dataclass
class MadridZone:
    """
    Actual geographic area of ​​Madrid.

    demand_weight: relative weight of order demand.
    More commercial areas (City Center, Salamanca) have higher values,
    generating the asymmetry (skew) of required demand en PARTE 1.
    """
    zone_id:       str
    display_name:  str
    demand_weight: float
    lat_center:    float
    lon_center:    float
    lat_radius:    float = 0.012
    lon_radius:    float = 0.012


# Real areas of Madrid with asymmetric demand weights.
MADRID_ZONES: List[MadridZone] = [
    MadridZone("CENTER",      "Center",      3.5, 40.4168, -3.7038),
    MadridZone("SALAMANCA",   "Salamanca",   3.0, 40.4256, -3.6844),
    MadridZone("CHAMBERI",    "Chamberí",    2.5, 40.4374, -3.7044),
    MadridZone("MALASANA",    "Malasaña",    2.8, 40.4258, -3.7076),
    MadridZone("LAVAPIES",    "Lavapiés",    2.0, 40.4090, -3.7030),
    MadridZone("RETIRO",      "Retiro",      1.8, 40.4120, -3.6844),
    MadridZone("TETUAN",      "Tetuán",      1.5, 40.4580, -3.7044),
    MadridZone("MONCLOA",     "Moncloa",     1.3, 40.4340, -3.7200),
    MadridZone("VALLECAS",    "Vallecas",    1.0, 40.3870, -3.6650),
    MadridZone("CARABANCHEL", "Carabanchel", 1.0, 40.3900, -3.7300),
]

# Quick lookup zone_id → MadridZone
ZONE_MAP: Dict[str, MadridZone] = {z.zone_id: z for z in MADRID_ZONES}


# =============================================================================
# ENTITY POOL
# =============================================================================

class EntityPool:
    """
    Manage the fixed set of restaurants and couriers in the simulation.
    
    Couriers have an ONLINE/ASSIGNED status to model real-world fleet availability. 
    `assign_courier()` and `release_courier()` maintain this status.
    """

    def __init__(self, config: Config):
        self.config       = config
        self.restaurants  = self._create_restaurants()
        self.couriers     = self._create_couriers()
        self._available: List[str] = list(self.couriers.keys())

    # ── Creation ──────────────────────────────────────────────────────────

    def _create_restaurants(self) -> Dict[str, Dict]:
        out = {}
        for i in range(self.config.num_restaurants):
            zone = random.choice(MADRID_ZONES)
            rid  = f"REST_{i + 1:03d}"
            out[rid] = {
                "restaurant_id": rid,
                "zone_id":       zone.zone_id,
                "name":          f"Rest. {zone.display_name} #{i + 1}",
            }
        return out

    def _create_couriers(self) -> Dict[str, Dict]:
        """
        Realistic vehicle distribution for Madrid:
        50% bicycles (immune to traffic), 35% motorcycles, 15% cars (highly affected).
        """
        out      = {}
        vehicles = [VehicleType.BIKE, VehicleType.SCOOTER, VehicleType.CAR]
        weights  = [0.50, 0.35, 0.15]
        for i in range(self.config.num_couriers):
            cid     = f"COURIER_{i + 1:03d}"
            vehicle = random.choices(vehicles, weights=weights, k=1)[0]
            zone    = random.choice(MADRID_ZONES)
            out[cid] = {
                "courier_id":    cid,
                "vehicle_type":  vehicle.value,
                "home_zone":     zone.zone_id,
                "status":        "ONLINE",
                "current_order": None,
            }
        return out

    # ──Availability management─────────────────────────────────────────

    def get_random_restaurant(self) -> Dict:
        return random.choice(list(self.restaurants.values()))

    def assign_courier(self) -> Optional[str]:
        """Assigns a free courier. Returns None if none are available."""
        if not self._available:
            return None
        cid = random.choice(self._available)
        self._available.remove(cid)
        self.couriers[cid]["status"] = "ASSIGNED"
        return cid

    def release_courier(self, courier_id: str) -> None:
        """Return a courier to the pool after completing or canceling the order."""
        c = self.couriers.get(courier_id)
        if c is None:
            return
        c["status"]        = "ONLINE"
        c["current_order"] = None
        if courier_id not in self._available:
            self._available.append(courier_id)
