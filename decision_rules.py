"""
Coffee Profile Decision Rules (Rule-based + Point Scoring)
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional


# =================== label candidates ===================

ACIDITY = ("low", "medium", "high")
CAFFEINE = ("low", "medium", "high")
ROAST = ("light", "medium", "dark")
FLAVOR = ("nutty_chocolate", "sweet_caramel", "fruity", "floral", "spicy")
PROCESS = ("washed", "natural", "honey", "anaerobic", "any")
BREW = ("filter", "espresso", "both")


# =================== User input schema ===================

@dataclass
class UserPref:
    stomach_sensitivity: str  # "low"|"medium"|"high"
    caffeine_sensitivity: str  # "low"|"medium"|"high"
    time_of_day: str  # "morning"|"afternoon"|"evening"
    purpose: str  # "focus"|"balanced"|"calm"
    flavor_direction: List[str]  # pilih 1-2 dari FLAVOR
    brew_method: str  # "filter"|"espresso"|"both"


# =================== Scoring utilities ===================

def init_scoreboard() -> Dict[str, Dict[str, int]]:
    return {
        "acidity": {k: 0 for k in ACIDITY},
        "caffeine": {k: 0 for k in CAFFEINE},
        "roast": {k: 0 for k in ROAST},
        "flavor": {k: 0 for k in FLAVOR},
        "process": {k: 0 for k in PROCESS},
        "brew": {k: 0 for k in BREW},
        # derived/helper:
        "bean_pref": {"arabica": 0, "blend": 0, "robusta": 0},
    }


def apply_delta(
    scores: Dict[str, Dict[str, int]],
    deltas: Dict[str, Dict[str, int]],
) -> None:
    
    for dim, changes in deltas.items():
        if dim not in scores:
            continue
        for label, delta in changes.items():
            if label in scores[dim]:
                scores[dim][label] += delta

def argmax_label(score_map: Dict[str, int]) -> str:
    # pilih label skor tertinggi; kalau seri, pilih yang "lebih aman" via urutan list
    # (bisa disesuaikan)
    best = None
    best_score = None
    for k, v in score_map.items():
        if best is None or v > best_score:
            best, best_score = k, v
    return best


def top_reasons(reasons: List[Tuple[str, int]], k: int = 4) -> List[str]:
    # reasons format: (text, priority_score)
    reasons_sorted = sorted(reasons, key=lambda x: x[1], reverse=True)
    return [r[0] for r in reasons_sorted[:k]]


# =================== RULES DEFINITION ===================

# Agar rapi, setiap rule punya:
# - deltas: skor yang ditambahkan
# - reason: teks penjelasan
# - reason_weight: prioritas alasan (biar yang penting muncul dulu)

Rule = Tuple[Dict[str, Dict[str, int]], str, int]

RULES: Dict[str, Dict[str, Rule]] = {
    "stomach_sensitivity": {
    "high": (
        {
            "acidity": {"low": +4, "medium": +1, "high": -3},
            "roast": {"medium": +2, "dark": +2, "light": -2},

            # NEW: lock bean preference away from robusta
            "bean_pref": {"arabica": +3, "blend": +1, "robusta": -6},
        },
        "High stomach sensitivity → prioritize low acidity + smoother roast; avoid robusta-heavy options to reduce irritation risk.",
        110,
    ),
    "medium": (
        {
            "acidity": {"medium": +3, "low": +1, "high": -1},
            "roast": {"medium": +2, "light": +1, "dark": +1},

            # NEW: mild nudge
            "bean_pref": {"arabica": +1, "blend": +1, "robusta": -1},
        },
        "Medium stomach sensitivity → keep profile balanced; prefer cleaner/smoother options.",
        70,
    ),
    "low": (
        {
            "acidity": {"high": +2, "medium": +2, "low": 0},
            "roast": {"light": +2, "medium": +2, "dark": 0},

            # OPTIONAL: allow anything (no strong push)
            "bean_pref": {"arabica": 0, "blend": +1, "robusta": +1},
        },
        "Low stomach sensitivity → you can explore brighter profiles; bean choice is flexible.",
        50,
    ),
    },

    "caffeine_sensitivity": {
    "high": (
        {
            "caffeine": {"low": +5, "medium": +1, "high": -6},

            # make robusta a hard-ish no
            "bean_pref": {"arabica": +4, "blend": +1, "robusta": -10},
        },
        "High caffeine sensitivity → strongly prioritize low caffeine; avoid robusta-heavy coffee.",
        120,
    ),
    "medium": (
        {
            "caffeine": {"medium": +3, "low": +1, "high": -2},
            "bean_pref": {"arabica": +2, "blend": +1, "robusta": -2},
        },
        "Medium caffeine sensitivity → aim for moderate caffeine (Arabica or light blend).",
        70,
    ),
    "low": (
        {
            "caffeine": {"high": +2, "medium": +2, "low": 0},
            "bean_pref": {"robusta": +2, "blend": +1, "arabica": 0},
        },
        "Low caffeine sensitivity → higher caffeine profiles are acceptable (blend/robusta allowed).",
        40,
    ),
    },

    "time_of_day": {
        "morning": (
            {"caffeine": {"high": +2, "medium": +2, "low": 0}, "roast": {"medium": +2, "light": +1}},
            "Morning coffee → allow medium–high caffeine for an energizing start.",
            60,
        ),
        "afternoon": (
            {"caffeine": {"medium": +2, "low": +1, "high": +1}, "roast": {"medium": +2}},
            "Afternoon coffee → keep caffeine moderate to avoid an afternoon crash.",
            50,
        ),
        "evening": (
            {"caffeine": {"low": +3, "medium": +1, "high": -2}, "roast": {"medium": +2, "dark": +1}},
            "Evening coffee → prioritize lower caffeine to reduce sleep disruption risk.",
            80,
        ),
    },

    "purpose": {
        "focus": (
            {"caffeine": {"high": +2, "medium": +2, "low": 0}, "roast": {"medium": +2}},
            "Purpose: focus → lean toward medium–high caffeine with a balanced roast.",
            60,
        ),
        "balanced": (
            {"caffeine": {"medium": +2}, "roast": {"medium": +2}, "acidity": {"medium": +2}},
            "Purpose: balanced → default to a versatile middle-profile (medium caffeine, medium roast, medium acidity).",
            50,
        ),
        "calm": (
            {"caffeine": {"low": +2, "medium": +1, "high": -1}, "acidity": {"low": +2, "medium": +1}},
            "Purpose: calm → prefer lower stimulation (lower caffeine, gentler acidity).",
            80,
        ),
    },
    
    "brew_method": {
        "filter": (
            {"brew": {"filter": +3, "both": +1}, "roast": {"light": +1, "medium": +2}},
            "Brew method: filter → filter-friendly profiles often shine at light–medium roast.",
            40,
        ),
        "espresso": (
            {"brew": {"espresso": +3, "both": +1}, "roast": {"medium": +2, "dark": +2}},
            "Brew method: espresso → espresso-friendly profiles often fit medium–dark roast.",
            40,
        ),
        "both": (
            {"brew": {"both": +3, "filter": +1, "espresso": +1}, "roast": {"medium": +2}},
            "Brew method: both → choose a flexible medium roast profile.",
            30,
        ),
    },
}

# Flavor direction: user bisa pilih 1-2, jadi kita apply per pilihan
FLAVOR_RULES: Dict[str, Rule] = {
    "nutty_chocolate": (
        {"flavor": {"nutty_chocolate": +3}, "roast": {"medium": +2, "dark": +2}, "process": {"washed": +1, "honey": +1, "any": +1}},
        "Flavor: nutty/chocolate → often pairs well with medium–dark roast; washed/honey can keep it clean and rounded.",
        55,
    ),
    "sweet_caramel": (
        {"flavor": {"sweet_caramel": +3}, "roast": {"medium": +2}, "process": {"honey": +2, "natural": +1, "any": +1}},
        "Flavor: sweet/caramel → typically fits medium roast; honey/natural processes can enhance sweetness.",
        55,
    ),
    "fruity": (
        {"flavor": {"fruity": +3}, "roast": {"light": +2, "medium": +1, "dark": -1}, "process": {"natural": +2, "anaerobic": +1, "washed": +1, "any": +1}},
        "Flavor: fruity → commonly found in light–medium roast; natural/anaerobic can emphasize fruit notes.",
        55,
    ),
    "floral": (
        {"flavor": {"floral": +3}, "roast": {"light": +2, "medium": +1}, "process": {"washed": +2, "any": +1}},
        "Flavor: floral → often shines in light roast; washed process tends to keep it clean and tea-like.",
        55,
    ),
    "spicy": (
        {"flavor": {"spicy": +3}, "roast": {"medium": +2, "dark": +2}, "process": {"washed": +1, "natural": +1, "any": +1}},
        "Flavor: spicy/earthy → commonly matches medium–dark roast; washed/natural can work depending on the bean.",
        55,
    ),
}


# =================== Main Function: building coffee profile ===================

def build_profile(user: UserPref) -> Dict[str, object]:
    scores = init_scoreboard()
    reasons: List[Tuple[str, int]] = []

    # Apply single-choice rules
    for field_name, value in [
        ("stomach_sensitivity", user.stomach_sensitivity),
        ("caffeine_sensitivity", user.caffeine_sensitivity),
        ("time_of_day", user.time_of_day),
        ("purpose", user.purpose),
        ("brew_method", user.brew_method),
    ]:
        field_rules = RULES.get(field_name, {})
        if value not in field_rules:
            continue
        deltas, reason, r_weight = field_rules[value]
        apply_delta(scores, deltas)
        reasons.append((reason, r_weight))

    # Apply multi-choice flavor rules (1-2)
    for f in user.flavor_direction:
        if f not in FLAVOR_RULES:
            continue
        deltas, reason, r_weight = FLAVOR_RULES[f]
        apply_delta(scores, deltas)
        reasons.append((reason, r_weight))

    # Resolve winners
    target_acidity = argmax_label(scores["acidity"])
    target_caffeine = argmax_label(scores["caffeine"])
    target_roast = argmax_label(scores["roast"])
    target_flavor = argmax_label(scores["flavor"])
    target_process = argmax_label(scores["process"])
    target_brew = argmax_label(scores["brew"])

    # Derived: bean preference from bean_pref scoreboard
    bean_pref = argmax_label(scores["bean_pref"])  # "arabica"/"blend"/"robusta"

    # Optional safety: if process winner is "any" but user selected a flavor that suggests process, keep "any"
    if user.caffeine_sensitivity == "high" and bean_pref == "robusta":
        bean_pref = "arabica"

    if user.stomach_sensitivity == "high" and bean_pref == "robusta":
        bean_pref = "arabica"

    return {
        "profile": {
            "acidity_level": target_acidity,
            "caffeine_tendency": target_caffeine,
            "roast_level": target_roast,
            "flavor_direction": target_flavor,
            "process_preference": target_process,
            "brew_suitability": target_brew,
            "bean_preference": bean_pref,
        },
        "scores": scores,  # for debugging/analysis (bisa disembunyikan di UI)
        "reasons": top_reasons(reasons, k=4),
        "disclaimer": "This is a preference-based recommendation and not medical advice.",
    }
