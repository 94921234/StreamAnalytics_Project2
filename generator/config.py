"""
  config.py — Centralized generator configuration.
  Contains:
    · EdgeCaseProbabilities — injection probabilities for each edge case
    · PromotionPeriod — definition of promotion periods
    · Config — main dataclass with all parameters
    · load_config_from_file — loads Config from an external JSON file
    
  CONFIGURABILITY:
  All relevant parameters are here. To change the generator's behavior, edit this class or 
  provide a config.json file in the CLI.
"""

import json
from dataclasses import dataclass, field
from typing import Dict, List


# =============================================================================
# CONFIGURATION OF EDGE CASES
# =============================================================================

@dataclass
class EdgeCaseProbabilities:
    """
    Injection probabilities for each edge case.
    All values ​​are floating-point values ​​in [0.0, 1.0].
    Adjust these values ​​to control the "dirtiness" of the dataset:
      • Low values ​​→ clean dataset, few anomalies
      • High values ​​→ noisy dataset, useful for stress-testing the pipeline
    """
    out_of_order:                float = 0.05   # [EC1] late arrivals to broker
    duplicate:                   float = 0.03   # [EC2] same event_id issued 2x
    missing_step:                float = 0.04   # [EC3] skip a state in the FSM
    impossible_duration:         float = 0.02   # [EC4] physically absurd times
    courier_offline_mid_delivery: float = 0.03  # [EC5] courier falls while ASSIGNED


# =============================================================================
# PROMOTION PERIOD
# =============================================================================

@dataclass
class PromotionPeriod:
    """
    Define a promotional period with an extra demand multiplier.
    It is applied to the base hourly demand Gaussian curve.
    """
    start_hour:        int
    end_hour:          int
    demand_multiplier: float
    name:              str = ""


# =============================================================================
# PRINCIPAL CONFIGURATION
# =============================================================================

@dataclass
class Config:
    """
    ╔══════════════════════════════════════════════════════════════════════╗
    ║  CONFIGURABILITY                                                     ║
    ║                                                                      ║
    ║  Parameter overriding (precedence order):                            ║
    ║    1. Default values ​​for this dataclass                              ║
    ║    2. JSON file →  python main.py --config config.json               ║
    ║    3. CLI flags →  python main.py --num-orders 2000                  ║
    ╚══════════════════════════════════════════════════════════════════════╝
    """

    # ── Entitiess ────────────────────────────────────────────────────────
    num_restaurants: int = 20
    num_couriers:    int = 50
    num_orders:      int = 500

    # ── Hourly Demand  ───────────────────────────────────────────────────
    demand_surge_multipliers: Dict[int, float] = field(default_factory=lambda: {
        13: 2.5, 14: 3.0, 15: 2.0,   # lunch 
        20: 2.8, 21: 3.5, 22: 2.5,   # dinner 
        23: 1.5,  0: 1.2,             # late night 
    })
    weekend_demand_multiplier: float = 1.4   # Saturday/Sunday: +40 % demand
    cancellation_probability:  float = 0.08  # 8 % of cancelled events

    # ── Promotions ───────────────────────────────────────────────────────
    promotion_periods: List[PromotionPeriod] = field(default_factory=lambda: [
        PromotionPeriod(12, 16, 1.5, "Lunch Deal"),
        PromotionPeriod(20, 23, 1.3, "Dinner Special"),
    ])

    # ── Simulation ───────────────────────────────────────────────
    output_dir:       str = "output"
    simulation_start: str = "2025-01-13 08:00:00"   
    simulation_hours: int = 16

    # ── Edge Cases ────────────────────────────────────────────────────────
    edge_cases: EdgeCaseProbabilities = field(
        default_factory=EdgeCaseProbabilities
    )


# =============================================================================
# LOAD JSON FILE
# =============================================================================

def load_config_from_file(path: str) -> Config:
    """
    Load the configuration from a JSON file.
    Only overwrite the existing fields; the rest will retain their default values.
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    cfg = Config()

    simple_fields = [
        "num_restaurants", "num_couriers", "num_orders",
        "weekend_demand_multiplier", "cancellation_probability",
        "output_dir", "simulation_start", "simulation_hours",
    ]
    for field_name in simple_fields:
        if field_name in data:
            setattr(cfg, field_name, data[field_name])

    # Las claves del JSON son strings; convertimos a int
    if "demand_surge_multipliers" in data:
        cfg.demand_surge_multipliers = {
            int(k): v for k, v in data["demand_surge_multipliers"].items()
        }

    if "promotion_periods" in data:
        cfg.promotion_periods = [
            PromotionPeriod(**p) for p in data["promotion_periods"]
        ]

    if "edge_cases" in data:
        cfg.edge_cases = EdgeCaseProbabilities(**data["edge_cases"])

    return cfg
